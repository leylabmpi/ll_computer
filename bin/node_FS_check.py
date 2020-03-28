#!/usr/bin/env python
from __future__ import print_function
import sys,os
import time
import signal
import shutil
import logging
import getpass
import argparse
import tempfile
import functools
import multiprocessing as mp
from subprocess import Popen, PIPE, run

desc = 'Check whether file systems are mounted on cluster nodes'
epi = """DESCRIPTION:
* Lists all nodes (qconf -sel)
* For each node (in parallel)
  * Submits a qsub job to that node
  * The job checks for /tmp/global/ and /tmp/global2/
* A report of all nodes is written to STDOUT

"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('-t', '--tmpdir', type=str,
                    default='/ebio/abt3_projects/temp_data/node_FS_check',
                    help='Directory for temp files (default: %(default)s)')  
parser.add_argument('-a', '--acct-file', type=str,
                    default='/var/lib/gridengine/default/common/accounting',
                    help='accounting file (default: %(default)s)')
parser.add_argument('-p', '--procs', type=int, default=1,
                    help='Number of parallel jobs (default: %(default)s)')                    
parser.add_argument('-m', '--max', type=int, default=55,
                    help='Max number of minutes to wait for a job to complete (default: %(default)s)')
parser.add_argument('-e', '--email', type=str,
                    default=getpass.getuser() + '@tuebingen.mpg.de',
                    help='email for notifications (default: %(default)s)')                    
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def list_nodes():
    cmd = ['qconf',  '-sel']
    p = Popen(cmd, stdout=PIPE)
    output, err = p.communicate()
    nodes = [x for x in output.decode().split('\n') if x != '']
    return nodes

def acct_query(job_id, acct_file):
    cmd = ['tac', acct_file]
    p = Popen(cmd, stdout=PIPE)
    output, err = p.communicate()

    for x in output.decode().split('\n'):
        x = x.split(':')
        try:
            if x[5] == job_id:
                return x[11]
        except IndexError:
            pass
    return None

class TimeoutError(Exception):
    pass

def handler(signum, frame):
    raise TimeoutError('Timeout')

def check_mount(node, tmpdir, acct_file, timeout):
    job = '''#!/bin/bash
#$ -pe parallel 1
#$ -o {job_out}
#$ -j y
#$ -cwd

> {job_out}

EC=0
mountpoint -q /tmp/global/ || EC=1
if [ $EC != 0 ]; then
  echo "WARNING: /tmp/global/ NOT mounted"
else
  echo "/tmp/global/ mounted"
fi

EC=0
mountpoint -q /tmp/global2/ || EC=1
if [ $EC != 0 ]; then
  echo "WARNING: /tmp/globa2/ NOT mounted"
else
  echo "/tmp/globa2/ mounted"
fi 

'''
    job_out_file = os.path.join(tmpdir, 'qsub_{}_output.txt'.format(node))
    job = job.format(job_out=job_out_file)
    
    job_file = os.path.join(tmpdir, 'qsub_{}.sh'.format(node))
    with open(job_file, 'w') as outF:
        outF.write(job)

    logging.info('{}: submitting job: {}'.format(node, job_file))
    cmd = ['qsub', job_file]
    p = Popen(cmd, stdout=PIPE)
    output, err = p.communicate()

    if p.returncode != 0:
        msg = 'qsub job for {} returned exit code: {}'
        raise IOError(msg.format(node, p.returncode))

    # job id
    job_id = output.decode().split(' ')[2]

    # timeout signal
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout * 60)
    
    # wait for job id to appear in qacct
    try_cnt = 0 
    try:
        while(1):
            logging.info('{}: waiting for job to finish'.format(node))
            exit_code = acct_query(job_id, acct_file)
            try:
                exit_code = int(exit_code.rstrip())
            except AttributeError:
                pass
            if exit_code is None:
                time.sleep(5)
                continue
            elif exit_code == 0:
                logging.info('{}: qsub job finished with exit code: 0'.format(node))
                break
            elif exit_code != 0:
                msg = '{}: qsub job finished with exit code: {}'
                logging.warning(msg.format(node, exit_code))
                return '{}: NA'.format(node)
            else:
                time.sleep(8)
            try_cnt += 1
            if try_cnt >= 750:
                msg = '{}: aborting after {} tries'
                logging.warning(msg.format(node, try_cnt))
                return '{}: NA'.format(node)                
    except TimeoutError:
        msg = '{}: Timeout at {} min'
        logging.warning(msg.format(node, timeout))
        return '{}: NA'.format(node)

    # kill qsub job if it still exists
    cmd = 'qdel {}'.format(job_id)
    p = Popen(cmd, stdout=PIPE)
    output, err = p.communicate()    
    
    # read output
    ret = []
    with open(job_out_file) as inF:
        for line in inF:            
            line = line.rstrip()
            ret.append('{}: {}'.format(node, line))
    return '\n'.join(ret)

def send_email(body, email):
    # email
    title = 'Cluster node FS warning report'
    cmd = "echo '{body}' | mutt -s '{title}' -- {email}"
    cmd = cmd.format(body=body, title=title, email=email)
    run(cmd, shell=True)
    
def main(args):    
    # tmp dir
    if not os.path.isdir(args.tmpdir):
        os.makedirs(args.tmpdir)

    # list of nodes on the cluster
    nodes = list_nodes()
    
    # simple qsub job to check for mount
    if args.procs > 1:
        pool = mp.Pool(args.procs)
        func = functools.partial(check_mount, tmpdir=args.tmpdir,
                                 acct_file=args.acct_file, timeout=args.max)
        ret = pool.map(func, nodes)
    else:
        ret = [check_mount(node, args.tmpdir, args.acct_file, args.max) \
               for node in nodes]

    # removing tmpdir
    shutil.rmtree(args.tmpdir)

    # checking for problem nodes & sending email if problem
    if any(['NOT mounted' in x for x in ret]):
        send_email('\n'.join(ret), args.email)
    
    # writing report
    for line in ret:
        print(line)

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
