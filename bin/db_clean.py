#!/usr/bin/env python
from __future__ import print_function
import sys,os
import re
import argparse
import logging
import datetime
import sqlite3

desc = 'Removing old records from the db'
epi = """DESCRIPTION:
Removing all old records from the database
"""
parser = argparse.ArgumentParser(description=desc,
                                 epilog=epi,
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-d', '--db-file', metavar='db_file', type=str,
                    default='/ebio/abt3_projects/databases_no-backup/ll_computer/ll_computer.db',
                     help='sqlite3 database file (default: %(default)s)')
parser.add_argument('-t', '--time', type=int, default=30,
                    help='Records older than this many time units will be removed (default: %(default)s)')
parser.add_argument('-u', '--units', type=str, default='days',
                    help='Time units to use for --time (default: %(default)s)')
parser.add_argument('--version', action='version', version='0.0.1')

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def list_tables(db_c):
    db_c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = db_c.fetchall()
    return [list(x)[0] for x in tables]

def clean_db(table_id, db_c, time, units):
    sql = "DELETE FROM {table} WHERE time < DATETIME('now', 'localtime', '-{time} {units}')"
    db_c.execute(sql.format(table=table_id, time=time, units=units))

def main(args):
    # sql connection
    conn = sqlite3.connect(args.db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    # listing tables
    tables = list_tables(c)
    # querying tables
    for table in tables:
        clean_db(table, c, args.time, args.units)
    # closing connection
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
