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
                    default='/ebio/abt3_projects2/databases_no-backup/ll_computer/ll_computer.db',
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
    tries = 0
    maxtries = 3
    while(1):
        tries += 1
        if tries > maxtries:
            msg = 'Exceeded {} tries to write to db. Giving up'
            logging.warning(msg.format(maxtries))
            break
        try:
            db_c.executemany('INSERT INTO server_load VALUES (?,?,?)', vals)    
            break
        except sqlite3.OperationalError:
            time.sleep(1)
            continue

def read_and_write(infile, label, db_file):
    # sql connection
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    # parsing file and writing to database    
    tries = 0
    maxtries = 3
    while 1:
        tries += 1
        if tries > maxtries:
            msg = 'Exceeded {} tries to open {}. Giving up\n'
            sys.stderr.write(msg.format(maxtries, infile))
            break
        try:
            cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(infile) as inF:
                for line in inF:
                    line = line.rstrip()
                    if line != '':
                        write_to_db([[label, cur_time, line]], c)
            break
        except IOError:
            time.sleep(0.5)
            continue
    
    # db connection close
    for i in range(5):
        try:
            conn.commit()
            break
        except sqlite3.OperationalError:
            time.sleep(0.5)
            pass
    conn.close()
    
def main(args):
        
    # parsing input and writing database
    in_files = args.in_files.split(',')
    labels = args.label.split(',')
    if len(in_files) != len(labels):
        raise ValueError('The number of labels doesn\'t match the number of input files')
    ## per-file parsing and writing to db
    for in_file,label in zip(in_files, labels):
        read_and_write(in_file, label, args.db_file)
            
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
