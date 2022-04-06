#!/usr/bin/env python
from __future__ import print_function
import sys,os
import argparse
import logging
import glob
import time
import datetime
import shutil
import subprocess
import sqlite3

desc = 'Getting project sizes for Andre Noll\'s user-info git repo'
epi = """DESCRIPTION:
Getting project sizes from Andre Noll's "user-info" git repo 
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                    help='sqlite3 database file (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def clone_repo():
    if os.path.isdir('user-info'):
        shutil.rmtree('user-info')
    cmd = 'git clone git://ilm.eb.local/user-info'
    try:
        with open(os.devnull, 'w') as DNULL:
            res = subprocess.run(cmd, check=True, shell=True,
                                 stdout=subprocess.PIPE,
                                 stderr=DNULL)
    except subprocess.CalledProcessError as e:
        raise e
    if not os.path.isdir('user-info'):
        raise IOError('user-info directory not found!')

def parse_proj_desc():
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    files = glob.glob(os.path.join('user-info', 'projects', 'abt3', '*'))
    sizes = []    
    for F in files:
        basename = os.path.split(F)[1]
        size = 0
        inodes = 0
        with open(F) as inF:
            for line in inF:
                if line.startswith('size:'):                    
                    size = line.split(' ')[-1].rstrip()
                    denom = 1000 if size.endswith('G') else 1
                    size = float(size.rstrip('TG')) / denom
                if line.startswith('inodes:'):
                    inodes = line.split(' ')[-1].rstrip().upper()
                    multi = 1
                    if inodes.endswith('K'):
                        multi = 1e3
                    elif inodes.endswith('M'):
                        multi = 1e6
                    elif inodes.endswith('G'):
                        multi = 1e9
                    inodes = int(inodes.rstrip('KMG')) * multi
        if inodes < 1:
            inodes = size * 1e6
        sizes.append([cur_time,basename,size,inodes/1e3])        
    # clean up 
    if os.path.isdir('user-info'):
        shutil.rmtree('user-info')
    return sizes
    
def write_to_db(vals, db_c):
    """
    Writing to sqlite3 db
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
            db_c.executemany('INSERT INTO project_sizes VALUES (?,?,?,?)', vals)    
            break
        except sqlite3.OperationalError:
            time.sleep(0.5)
            continue
        
def main(args):
    # cloning repo
    clone_repo()
    # parsing project sizes
    sizes = parse_proj_desc()
    # writing to database
    ## connection
    conn = sqlite3.connect(args.db_file, timeout=2)
    c = conn.cursor()
    ## writing
    write_to_db(sizes, c)
    ## closing 
    conn.commit()
    conn.close()


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
