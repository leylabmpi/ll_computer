#!/usr/bin/env python
from __future__ import print_function
import sys,os
import re
import time
import argparse
import logging
import datetime
import sqlite3

desc = 'Parsing Andre\'s disk-usage files'
epi = """DESCRIPTION:
Parsing Andre's disk-usage files and writing info
to sqlite3 database 
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                     help='sqlite3 database file (default: %(default)s)')
parser.add_argument('-u', '--usage-files', type=str,
                    default='/tmp/global2/disk-usage,/ebio/abt3_projects/disk-usage,/ebio/abt3/disk-usage,/ebio/abt3_scratch/disk-usage',
                     help='comma delim list of path to disk-usage file(s) (default: %(default)s)')
parser.add_argument('-l', '--label', type=str,
                    default='tmp-global2,abt3-projects,abt3-home,abt3-scratch',
                    help='comma-delim list of label for file systems assessed (default: %(default)s)')
parser.add_argument('-i', '--inodes', action='store_true', default=False,
                     help='Parse inodes instead of disk-usage (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def parse_section(inF, cur_time, label, data_type):
    regex = re.compile(' +')
    vals = []
    for line in inF:
        line = regex.split(line.rstrip())
        if line[0] == '':
            break
        elif line[0].startswith('~~~~~~~~'):
            continue
        if line[1].rstrip('*') == '':
            line = [line[0], line[2], line[3]]
        line[2] = line[2].lstrip('(').rstrip('%)')
        line = [data_type, label, cur_time] + line
        vals.append([str(x) for x in line])
    return vals

def write_to_db(vals, db_c):
    """Writing to sqlite3 db
    """
    tries = 0
    maxtries = 3
    while(1):
        tries += 1
        if tries > maxtries:
            msg = 'Exceeded {} tries to write to db. Giving up'
            logging.warning(msg.format(maxtries))
            break
        try:
            db_c.executemany('INSERT INTO disk_usage VALUES (?,?,?,?,?,?)', vals)    
            break
        except sqlite3.OperationalError:
            time.sleep(0.5)
            continue
    
def main(args):
    # data type
    if args.inodes:
        which_section = ['project inode usage (unit:', 'inode usage (unit:']
        data_type = 'inodes'
    else:
        which_section = ['project disk usage (unit:', 'disk usage (unit:']
        data_type = 'disk usage'
        
    # parsing input and writing database
    usage_files = args.usage_files.split(',')
    labels = args.label.split(',')
    if len(usage_files) != len(labels):
        raise ValueError('The number of labels doesn\'t match the number of usage files')
    
    for usage_file,label in zip(usage_files, labels):
        cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # sql connection
        conn = sqlite3.connect(args.db_file, timeout=2)
        c = conn.cursor()
        # parsing file and writing to database
        with open(usage_file) as inF:
            for line in inF:
                line = line.rstrip()
                if any([line.startswith(x) for x in which_section]):
                    vals = parse_section(inF, cur_time, label, data_type)
                    if len(vals) > 0:
                        write_to_db(vals, c)
        # closing 
        conn.commit()
        conn.close()
    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
