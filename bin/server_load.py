#!/usr/bin/env python
from __future__ import print_function
import sys,os
import re
import time
import argparse
import logging
import datetime
import sqlite3

desc = 'Parsing writing server load data to sqlite3 db'
epi = """DESCRIPTION:
Writing server I/O load info to sqlite3 db 
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                     help='sqlite3 database file (default: %(default)s)')
parser.add_argument('-i', '--in-files', type=str,
                    default='/ebio/abt3_projects/server-load,/ebio/abt3_scratch/server-load,/tmp/global2/server-load',
                     help='comma delim list of path to disk-usage file(s) (default: %(default)s)')
parser.add_argument('-l', '--label', type=str,
                    default='LUX,FOX,tmp-global2',
                    help='comma-delim list of label for file systems assessed (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def write_to_db(vals, db_c):
    """Writing to sqlite3 db
    """
    db_c.executemany('INSERT INTO server_load VALUES (?,?,?)', vals)    

def read_and_write(infile, label, c):
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(infile) as inF:
        for line in inF:
            line = line.rstrip()
            if line != '':
                write_to_db([[label, cur_time, line]], c)
    
def main(args):
    # sql connection
    conn = sqlite3.connect(args.db_file)
    c = conn.cursor()
        
    # parsing input and writing database
    in_files = args.in_files.split(',')
    labels = args.label.split(',')
    if len(in_files) != len(labels):
        raise ValueError('The number of labels doesn\'t match the number of input files')

    tries = 0
    for in_file,label in zip(in_files, labels):
        while(1):
            tries += 1
            if tries > 15:
                logging.warning('Exceeded 15 tries. Giving up')
                break
            try:
                read_and_write(in_file, label, c)
                break
            except (IOError, sqlite3.OperationalError) as e:
                time.sleep(3)
                continue
    tries = 0
    while(1):
        tries += 1
        if tries > 5:
            logging.warning('Exceeded 5 tries to commit changes. Giving up')
        break
        try:
            conn.commit()
        except (IOError, sqlite3.OperationalError) as e:
            time.sleep(2)
            continue
    conn.close()
            
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
