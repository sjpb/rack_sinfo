#!/usr/bin/env python
""" Count the number of nodes in each rack which are usable on the CSD3's cclake partition.

    This assumes racks are continous sets of 56 nodes.

    Usage:
        ./count_nodes.py [STATES]
    
    where STATES is a comman-separated list of slurm node states to match - NB suffixes e.g. "*" must be included to match.

    By default STATES is 'idle,available,alloc'.
"""

import subprocess, sys

DEFAULT_STATES=['idle', 'available', 'alloc']
NODES_PER_RACK = 56

def show_hostlist(nodes):
    """ Return a hostlist expression given a list of nodes """
    scontrol_cmd = ['scontrol', 'show', 'hostlist', ','.join(nodes)]
    hostlist = subprocess.run(scontrol_cmd, capture_output=True, text=True).stdout.strip()
    return hostlist


sinfo_cmd = ['sinfo', '--Node', '--partition=cclake', '--noheader',]  # NODELIST   NODES PARTITION STATE

if __name__ == '__main__':
    if len(sys.argv) == 1:
        states = DEFAULT_STATES
    else:
        states = sys.argv[1].split(',')
    print('Searching for the folowing states: %s' % states)
    partition = 'cclake'
    racks = {}

    for line in subprocess.run(sinfo_cmd, capture_output=True, text=True).stdout.split('\n'):
        if not line:
            break
        hostname, numnode, partition, state = line.split()
        nodeid = int(hostname.rsplit('-', 1)[-1])
        rackid = int((nodeid-1)/NODES_PER_RACK) + 1
        if rackid not in racks:
            racks[rackid] = {'all':[], 'reqd':[]}
        racks[rackid]['all'].append(hostname)
        if state in states:
            racks[rackid]['reqd'].append(hostname)

    for rackid, info in racks.items():

        all_nodes = show_hostlist(info['all'])
        reqd_nodes = show_hostlist(info['reqd'])
        if len(info['reqd']) > 1:
            print('Rack %s (%s): %i nodes - %s' % (rackid, all_nodes, len(info['reqd']), reqd_nodes))
        elif len(info['reqd']) == 1:
            print('Rack %s (%s): %i node - %s' % (rackid, all_nodes, len(info['reqd']), reqd_nodes))
        else:
            print('Rack %s (%s): 0 nodes' % (rackid, all_nodes))