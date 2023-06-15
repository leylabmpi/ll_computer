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
                    default='/ebio/abt3_projects2/databases_no-backup/ll_computer/ll_computer.db',
                     help='sqlite3 database file (default: %(default)s)')
parser.add_argument('-u', '--usage-files', type=str,
                    default='/tmp/global2/disk-usage,/ebio/abt3_projects/disk-usage,/ebio/abt3/disk-usage,/ebio/abt3_scratch/disk-usage',
                     help='comma delim list of path to disk-usage file(s) (default: %(default)s)')
parser.add_argument('-l', '--label', type=str,
                    default='tmp-global2,abt3-projects,abt3-home,abt3-scratch',
                    help='comma-delim list of label for file systems assessed (default: %(default)s)')
#parser.add_argument('-i', '--inodes', action='store_true', default=False,
#                     help='Parse inodes instead of disk-usage (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.2')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


#def parse_section(inF, cur_time, label, data_type):
def parse_section(inF, cur_time, label):    
    regex = re.compile(' +')
    vals = []
    for line in inF:
        #line = regex.split(line.rstrip())
		# split line by any of the symbols: space, tab or slash
        line = re.split(' |\t|/', line.rstrip().strip(' '))
        # remove empty values
        line = [x for x in line if x != '']
        #print(line)
        # drop the first value in line if it's empty        
        if line[0].startswith('~~~~~~~~'):
            continue
        if line[0] == '':
            line = line[1:]
        #if line[1].rstrip('*') == '':
        #    line = [line[0], line[2], line[3]]
        #line[2] = line[2].lstrip('(').rstrip('%)')
        #line = [data_type, label, cur_time] + line
        blocks = line[1].rstrip('G/')
        blocks_lim = line[2].rstrip('G')
        blocks_perc = 0 if blocks_lim == '0' else round(100*float(blocks)/float(blocks_lim))
        inodes = line[3].rstrip('k/')
        inodes_lim = line[4].rstrip('k/')
        inodes_perc = 0 if inodes_lim == '0' else round(100*float(inodes)/float(inodes_lim))        
        #line = [line[1], line[2], line[3]]
        line = [label, cur_time, line[0], blocks, blocks_perc, inodes, inodes_perc]
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
    #####################
	# AT: no sections anymore in new format of disk-usage since Dec 2021.
    # data type
    #if args.inodes:
    #    which_section = ['project inode usage (unit:', 'inode usage (unit:']
    #    data_type = 'inodes'
    #else:
    #    which_section = ['project disk usage (unit:', 'disk usage (unit:']
    #    data_type = 'disk usage'
        
    # parsing input and writing database
    usage_files = args.usage_files.split(',')
    labels = args.label.split(',')
    if len(usage_files) != len(labels):
        raise ValueError('The number of labels doesn\'t match the number of usage files')
    
    for usage_file,label in zip(usage_files, labels):
        #print([usage_file,label])
        cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # sql connection
        conn = sqlite3.connect(args.db_file, timeout=2)
        c = conn.cursor()
        # parsing file and writing to database
        with open(usage_file) as inF:
            for line in inF:
                line = line.rstrip()
                #print(line)
                #if(any([line.startswith(x) for x in which_section])):
                if(True):
                    vals = parse_section(inF, cur_time, label) #, data_type)
                    if len(vals) > 0:
                        #write_to_db(vals, c)
                        #print(vals)
						# get the first three values from each element of vals into a new array
                        vals_blocks = [x[:5] for x in vals]                        
                        vals_blocks = [['disk usage'] + x for x in vals_blocks]
                        write_to_db(vals_blocks, c)
                        vals_inodes = [x[0:3] + x[5:] for x in vals]
                        vals_inodes = [['inodes'] + x for x in vals_inodes]
                        write_to_db(vals_inodes, c)                        
        # closing 
        conn.commit()
        conn.close()
    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
