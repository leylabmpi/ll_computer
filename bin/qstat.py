#!/usr/bin/env python
from __future__ import print_function
import sys,os
import argparse
import logging
import datetime
import sqlite3
from subprocess import Popen, PIPE
import xml.etree.ElementTree as ET

desc = 'Write qstat output to sqlite3 db'
epi = """DESCRIPTION:
Writing qstat output to sqlite3 database
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                    help='sqlite3 database file (default: %(default)s)')
parser.add_argument('-g', '--group', type=str, default='abt3',
                    help='User group (default: %(default)s)')
parser.add_argument('-n', '--num-rows', type=int, default=40,
                    help='Number of rows (default: %(default)s)')
parser.add_argument('-s', '--sort-column', type=int, default=6,
                    help='Column to sort by (default: %(default)s)')
parser.add_argument('-H', '--header', action='store_true', default=False,
                    help='Include a header? (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def get_all_users(user_group):
    """Getting all users for user group
    """
    cmd = ['getent', 'group', user_group]
    p = Popen(cmd, stdout=PIPE)
    ret, err = p.communicate()
    ret = ret.decode('utf8').rstrip().split(':')[-1]
    ret = ret.split(',')
    if ret[0] == '':
       raise ValueError('No users found for group: {}'.format(user_group))        
    return(ret)

def write_to_db(vals, db_c):
    """Writing to sqlite3 db
    """
    db_c.executemany('INSERT INTO qstat VALUES (?,?,?,?,?,?,?,?,?)', vals)

def qstat_users(users, args):
    # sql connection
    conn = sqlite3.connect(args.db_file)
    c = conn.cursor()
    
    # qstat call
    cmd = '"{}"'.format(','.join(users))
    cmd = ['qstat', '-ext', '-xml', '-u', cmd]
    p = Popen(cmd, stdout=PIPE)
    ret, err = p.communicate()
    
    # parsing xml & writing to db
    cols = ['time', 'JB_job_number', 'JB_name', 'JB_owner', 'JB_department',
            'state', 'io_usage', 'cpu_usage', 'mem_usage']
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ## body
    vals = []
    root = ET.fromstring(ret)
    for child1 in root:
        for child2 in child1:
            job_info = {}
            for child3 in child2:
                if child3.tag in cols[5:]:
                    try:
                        job_info[child3.tag] = float(child3.text)
                    except (TypeError, ValueError) as e:
                        job_info[child3.tag] = child3.text
                else:
                    job_info[child3.tag] = child3.text                        
            row = [cur_time]
            for x in cols[1:]:
                try:
                    row.append(job_info[x])
                except KeyError:
                    row.append(None)
            vals.append(row)

    if len(vals) > 0:
        write_to_db(vals, c)
    conn.commit()
    conn.close()

def main(args):
    users = get_all_users(args.group)
    qstat_users(users, args)  
    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
