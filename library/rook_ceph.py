#!/usr/bin/python

# Copyright: (c) 2021, Ho Kim <ho.kim@smartx.kr>
# MIT License
from __future__ import (absolute_import, division, print_function)
import os
import shutil
import subprocess
import time
import urllib3
import yaml
__metaclass__ = type

DOCUMENTATION = r'''
---
module: rook_ceph

short_description: Simple Rook-Ceph Provisioning Tool

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form '2.5.0' and not '2.4'.
version_added: '1.0.0'

description: Simple Rook-Ceph Provisioning Tool

options:
    rook:
        description: Rook configuration
        required: false
        type: dict
        suboptions:
            version:
                description: Rook version
                required: true
                type: str
    ceph:
        description: Ceph configuration
        required: true
        type: dict
        suboptions:
            image:
                description: Ceph image configuration
                required: false
                type: dict
                suboptions:
                    user:
                        description: Ceph image's username
                        required: false
                        type: str
                        default: ceph
                    version:
                        description: Ceph image's version
                        required: false
                        type: str
            mode:
                description: Ceph's configuration mode
                required: false
                type: str
                choices:
                    - LVM
                    - RAW
                default: LVM
            osdsPerDevice:
                description: OSDs per Device
                required: false
                type: int
                default: 6
            nodes:
                description: Ceph cluster nodes
                required: false
                type: list
                suboptions:
                    name:
                        description: Node's name
                        required: true
                        type: str
                    metadata:
                        description: Ceph's metadata device.
                        required: true
                        type: str
                    volumes:
                        description: Ceph's storage devices.
                        required: true
                        type: list[str]

author:
    - Ho Kim (@kerryeon)
'''

EXAMPLES = r'''
- name: Deploy Rook-Ceph cluster
  kerryeon.ansible_rook_ceph.rook_ceph:
    deploy:
      rook:
        version: 1.5.12
      ceph:
        image:
          user: kerryeon
          version: 15.2.7
        mode: LVM
        osdsPerDevice: 3
        nodes:
          - name: my-node1
            metadata: /dev/disk/by-id/...
            volumes:
              - /dev/disk/by-id/...
'''

RETURN = ''' # '''


FILES = {
    'crds.yaml': 'crds.yaml',
    'common.yaml': 'common.yaml',
    'operator.yaml': 'operator.yaml',
    'cluster.yaml': 'cluster.yaml',
    'storageclass.yaml': 'csi/rbd/storageclass.yaml',
    'toolbox.yaml': 'toolbox.yaml',
}
SOURCE = '/tmp/rook-ceph'


def gather_facts():
    return {}


def deploy(params: dict):
    # rook
    rook: dict = params['rook']
    rook_version: str = rook['version']

    # ceph
    ceph: dict = params['ceph']
    ceph_force_cleanup: bool = ceph.get('forceCleanup') or False
    ceph_image: dict = ceph.get('image') or {}
    ceph_image_user: str = ceph_image.get('user') or 'ceph'
    ceph_image_version: str = ceph_image.get('version')
    ceph_mode: str = ceph['mode']
    ceph_osds_per_device: int = ceph.get('osdsPerDevice') or 6
    ceph_nodes: list[object] = ceph.get('nodes')

    # Download files
    os.makedirs(SOURCE, mode=0o755, exist_ok=True)
    http = urllib3.PoolManager()
    for file, src in FILES.items():
        url = f'https://raw.githubusercontent.com/rook/rook/v{rook_version}/cluster/examples/kubernetes/ceph/{src}'
        with open(f'{SOURCE}/{file}', 'wb') as out:
            r = http.request('GET', url, preload_content=False)
            shutil.copyfileobj(r, out)

    # Modify operator.yaml
    with open(f'{SOURCE}/operator.yaml', 'r') as f:
        context = list(yaml.load_all(f, Loader=yaml.SafeLoader))
        context[0]['data']['ROOK_ENABLE_DISCOVERY_DAEMON'] = 'true'
        # context[1]['spec']['template']['spec']['hostNetwork'] = True
        # for env in context[1]['spec']['template']['spec']['containers'][0]['env']:
        #     if env['name'] == 'ROOK_HOSTPATH_REQUIRES_PRIVILEGED':
        #         env['value'] = 'true'
        #         break
    with open(f'{SOURCE}/operator.yaml', 'w') as f:
        yaml.dump_all(context, f, Dumper=yaml.SafeDumper)

    # Modify cluster.yaml
    with open(f'{SOURCE}/cluster.yaml', 'r') as f:
        context = yaml.load(f, Loader=yaml.SafeLoader)
        spec = context['spec']

        # specify the fixed ceph version
        if ceph_image_user is not None and ceph_image_version is not None:
            spec['cephVersion']['image'] = f'{ceph_image_user}/ceph:v{ceph_image_version}'

        # specify the ceph cluster network plane
        # spec['network'] = {
        #     'provider': 'host',
        # }

        # specify the disks for ceph cluster
        storage = spec['storage']
        if 'config' not in storage or storage['config'] is None:
            storage['config'] = {}

        # if the nodes are specified
        if ceph_nodes:
            storage['useAllNodes'] = False
            storage['useAllDevices'] = False
            storage['deviceFilter'] = ''

            num_nodes = 0
            storage.setdefault('nodes', [])
            for node in ceph_nodes:
                # node
                node_name: str = node['name']
                node_metadata: str = node.get('metadata')
                node_volumes: list[str] = node['volumes']

                storage_config = {}
                storage_devices = []

                if not node_volumes:
                    print(f'Skipping Rook-Ceph Node: {node_name}')
                    continue

                # RAW mode
                if ceph_mode == 'RAW':
                    print('RAW mode is not supported yet')
                    return False
                # LVM mode (default)
                else:
                    storage_config['metadataDevice'] = node_metadata
                    for volume in node_volumes:
                        storage_devices.append({
                            'name': volume,
                            'config': {
                                'osdsPerDevice': str(ceph_osds_per_device),
                            },
                        })

                if storage_devices:
                    num_nodes += 1
                    storage['nodes'].append({
                        'name': node_name,
                        'config': storage_config,
                        'devices': storage_devices,
                    })

            num_nodes = min(3, num_nodes)

        # if the nodes are not specified
        else:
            storage['useAllNodes'] = True
            storage['useAllDevices'] = True

            # RAW mode
            if ceph_mode == 'RAW':
                print('RAW mode is not supported when nodes are not specified')
                return False
            # LVM mode (default)
            else:
                storage['config'] = {
                    'osdsPerDevice': str(ceph_osds_per_device),
                }
            num_nodes = 1

        num_mons = ((num_nodes + 1) // 2) * 2 - 1
        spec['mon']['count'] = num_mons
    with open(f'{SOURCE}/cluster.yaml', 'w') as f:
        yaml.dump(context, f, Dumper=yaml.SafeDumper)

    # Modify storageclass.yaml
    with open(f'{SOURCE}/storageclass.yaml', 'r') as f:
        context = list(yaml.load_all(f, Loader=yaml.SafeLoader))
        replicated = context[0]['spec']['replicated']
        replicated['size'] = num_nodes
        replicated['requireSafeReplicaSize'] = num_nodes > 2
    with open(f'{SOURCE}/storageclass.yaml', 'w') as f:
        yaml.dump_all(context, f, Dumper=yaml.SafeDumper)

    # Apply
    for file in FILES:
        os.system(f'kubectl apply -f {SOURCE}/{file}')
        if file.startswith('operator'):
            os.system(
                r'kubectl -n rook-ceph rollout status deploy/rook-ceph-operator')
            time.sleep(60)
        else:
            time.sleep(1)
    os.system(r'kubectl -n rook-ceph rollout status deploy/rook-ceph-tools')
    os.system(
        r'kubectl patch storageclass rook-ceph-block '
        r'-p \'{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}\''
    )

    # Finish
    return True


def reset(params: dict):
    # ceph
    ceph: dict = params['ceph']
    ceph_nodes: list[object] = ceph.get('nodes')

    for file in FILES:
        os.system(
            f'kubectl delete -f {SOURCE}/{file} --timeout=30s || true'
        )

    os.system(r'sudo dmsetup remove_all')
    os.system(r'sudo rm -rf /dev/ceph-*')
    os.system(r'sudo rm -rf /dev/mapper/ceph--*')
    os.system(r'sudo rm -rf /var/lib/rook/')
    # os.system(r'sudo rm -rf /var/lib/kubelet/plugins/')
    # os.system(r'sudo rm -rf /var/lib/kubelet/plugins_registry/')

    # estimate volumes
    # note: dependency "jq" must be installed, if nodes are not specified!
    if not ceph_nodes:
        node_volumes = subprocess.check_output([
            "/bin/bash", "-c",
            'lsblk --fs --json | '
            'jq -r \'.blockdevices[] | select(.children == null and .fstype == "LVM2_member") | .name\'',
        ]).decode('utf-8').split('\n')[:-1]
    else:
        import socket
        node_name = socket.gethostname()
        node = next(node for node in ceph_nodes if node['name'] == node_name)
        node_volumes: list[str] = node['volumes']

    # Cleanup LVMs
    for volume in node_volumes:
        if not volume.startswith('/dev/'):
            volume = f'/dev/{volume}'

        os.system(f'sudo wipefs --all {volume} && sync')
        os.system(f'sudo sgdisk --zap-all {volume} && sync')
        os.system(
            f'sudo dd if=/dev/zero of={volume} bs=1M count=100 oflag=direct,dsync && sync'
        )
        os.system(f'sudo blkdiscard {volume} && sync')
        os.system(f'sudo partprobe {volume} && sync')

    # Finish
    return True


argument_spec = {
    'gather_facts': {'type': 'bool', 'required': False, },
    'deploy': {'type': 'dict', 'required': False, },
    'reset': {'type': 'dict', 'required': False, },
}


def setup_module_object():
    from ansible.module_utils.basic import AnsibleModule
    module = AnsibleModule(argument_spec=argument_spec,
                           supports_check_mode=False)

    return module


def run_task(module):
    ret = {'changed': False, 'failed': False, 'ansible_facts': {}}

    arg_gather_facts = module.params['gather_facts']
    if arg_gather_facts:
        ret['ansible_facts'] = gather_facts()
        return ret

    arg_deploy = module.params['deploy']
    if arg_deploy:
        ret['changed'] = ret['changed'] or deploy(arg_deploy)
        return ret

    arg_reset = module.params['reset']
    if arg_reset:
        ret['changed'] = ret['changed'] or reset(arg_reset)
        return ret

    return ret


def main():
    module = setup_module_object()

    try:
        ret = run_task(module)
    except Exception as e:
        module.fail_json(msg='{0}: {1}'.format(type(e).__name__, str(e)))

    module.exit_json(**ret)


if __name__ == '__main__':
    main()
