#!/usr/bin/env python3.8
""" List nodes by rack.

    This assumes racks are continous sets of 56 nodes.

    Usage:
        ./count_nodes.py [options]
    
    Options:
       
    - --states STATES: filter to nodes in one of the given comma-separated slurm node states to match, or "any", default "idle". NB suffixes e.g. "*" must be included to match except for "any".
    - --racks RACKS: filter to nodes in the given comma-separated list of racks, or "all" (default)
    - --numnodes NUMNODES: filter to nodes in racks where this many nodes meet critera, default -1 means any number
    - --partitions PARTITION: filter to nodes in the given comma-separated list of partitions, default is "default" which selects the default paritition only
    - --format output: One of
        - csv: comma-separated list of matches nodes (default)
        - exclude: like csv but non-matching, suitable for sbatch "--exclude" directive
        - row: one item per row
        - count: output counts instead of items
    - --output ITEM: Item to output, one of:
        - hostname (default)
        - rack
        - u_loc
        - chassis_loc
        - partition
        - state
"""

import subprocess, sys

NODES_PER_RACK = 56

cmd_opts = {
    '--states':'idle',
    '--racks': 'all',
    '--numnodes': '-1',
    '--format': 'csv',
    '--partitions': 'default',
    '--output':'hostname',
    '--unique':'no',
}

def show_hostlist(nodes):
    """ Return a hostlist expression given a list of nodes """
    scontrol_cmd = ['scontrol', 'show', 'hostlist', ','.join(nodes)]
    hostlist = subprocess.run(scontrol_cmd, capture_output=True, text=True).stdout.strip()
    return hostlist

def get_nodes_info():

    nodes = []
    sinfo_cmd = ['sinfo', '--Node', '--noheader',]  # NODELIST   NODES PARTITION STATE
    for line in subprocess.run(sinfo_cmd, capture_output=True, text=True).stdout.split('\n'):
        if not line:
            break
        hostname, numnode, partition, state = line.split()
        if numnode != '1':
            exit('FATAL: Found %s nodes despite using --Node option, line from sbatch was %r' % (numnode, line))
        _, rackid, u_loc, chassis_loc = hostname.split('-') # e.g. cpu-h21a5-u7-svn2 # TODO: find a way to make general!
        nodes.append(dict(
            hostname=hostname,
            rack=rackid,
            u_loc=u_loc,
            chassis_loc=chassis_loc,
            partition=partition,
            state=state,
        ))
    return nodes

if __name__ == '__main__':

    cmd_opts.update(dict(zip(*[iter(sys.argv[1:])]*2)))
    all_nodes = get_nodes_info()
    
    if cmd_opts['--states'] != 'any':
        states = cmd_opts['--states'].split(',')
        filtered = [n for n in all_nodes if n['state'] in states]
    else:
        filtered = [n for n in all_nodes]
    if cmd_opts['--racks'] != 'all':
        racks = cmd_opts['--racks'].split(',')
        filtered = [n for n in filtered if n['rack'] in racks ]
    if cmd_opts['--partitions'] == 'default':
        filtered = [n for n in filtered if n['partition'][-1] == '*']
    else:
        partitions = cmd_opts['--partitions'].split(',')
        filtered = [n for n in filtered if n['partition'] in partitions]
    if cmd_opts['--numnodes'] != '-1':
        # group by rack:
        racks = {}
        for n in filtered:
            racks.setdefault(n['rack'], []).append(n)
        filtered = []
        for nodes in racks.values():
            if len(nodes) == int(cmd_opts['--numnodes']):
                filtered.extend(nodes)


    key = cmd_opts['--output']

    if cmd_opts['--format'] == 'exclude':
        excluded = [n[key] for n in all_nodes if n not in filtered]
        print(','.join(excluded))
    else:
        output = [n[key] for n in filtered]
        if cmd_opts['--unique'] == 'yes':
            output = list(set(output))
        if cmd_opts['--format'] == 'csv':
            print(','.join(output))
        elif cmd_opts['--format'] == 'row':
            print('\n'.join(output))
        elif cmd_opts['--format'] == 'count':
            print('all_nodes:', len(all_nodes))
            print('filtered:', len(output))
