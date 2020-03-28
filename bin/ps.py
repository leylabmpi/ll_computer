#!/usr/bin/env python
from __future__ import print_function
import sys,os
import re
import argparse
import logging
import datetime
import sqlite3
from socket import gethostname
from subprocess import Popen, PIPE

desc = 'Write ps output to sqlite3 database'
epi = """DESCRIPTION:
Getting table of jobs running on the VM.
Output table written to sqlite3 database.
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                     help='sqlite3 database file (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def write_to_db(vals, db_c):
    """Writing to sqlite3 db
    """
    db_c.executemany('INSERT INTO ps VALUES (?,?,?,?,?,?,?,?)', vals)    

def main(args):
    # compiling regex's
    regex = re.compile(r'^([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +([^ ]+) +(.+)')
    regex2 = re.compile(r'\t')

    # sql connection
    conn = sqlite3.connect(args.db_file)
    c = conn.cursor()
    
    # getting hostname
    hostname = gethostname()
    
    # ps call
    cmd = ['ps', '--no-headers', '-axww', '-o' 'uname:50,ppid,pid,etime,%cpu,%mem,args']
    p = Popen(cmd, stdout=PIPE)
    ret, err = p.communicate()

    # parsing output
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    vals = []
    for line in ret.decode().split('\n'):
        line = [hostname,
                cur_time,
                regex.sub(r'\1', line),
                regex.sub(r'\2', line),
                regex.sub(r'\3', line),
                regex.sub(r'\4', line),
                regex.sub(r'\5', line),
                regex.sub(r'\6', line)] 
        if line[1] != '':
            vals.append(line)
    if len(vals) > 0:
        write_to_db(vals, c)
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
