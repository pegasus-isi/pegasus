#!/usr/bin/env python
"""
A simple command-line tool to query and set schema version 
information in a stampede database.  Will also upgrade an 
existing database to a newer schema version.

Will need to be run as a user that has CREATE | ALTER TABLE 
permissions in the database.

Requires a standard SQLALchemy connection string.
"""

__rcsid__ = "$Id: schema_tool.py 29514 2012-01-23 18:22:03Z mgoode $"
__author__ = "Monte Goode"

import os
import sys
import logging
import subprocess

# use pegasus-config to get basic pegasus settings
bin_dir = os.path.join(os.path.normpath(os.path.join(os.path.dirname(sys.argv[0]))), "../../../bin")
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --noeoln --python"
lib_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]
pegasus_config = os.path.join(bin_dir, "pegasus-config") + " --noeoln --python-externals"
lib_ext_dir = subprocess.Popen(pegasus_config, stdout=subprocess.PIPE, shell=True).communicate()[0]

# Insert this directory in our search path
os.sys.path.insert(0, lib_ext_dir)
os.sys.path.insert(0, lib_dir)

from netlogger.analysis.schema.schema_check import ConnHandle, SchemaCheck
from netlogger.nllog import OptionParser, get_logger

def main():
    usage = "%prog {-c | -u} connString='required' mysql_engine='optional'"
    desc = ' '.join(__doc__.split())
    parser = OptionParser(usage=usage, description=desc)
    parser.add_option('-c', '--check', dest='schema_check', action='store_true',
                        default=False, help="Perform a schema check")
    parser.add_option('-u', '--upgrade', dest='upgrade', action='store_true',
                        default=False, help="Upgrade database to current version.")
    options, args = parser.parse_args(sys.argv[1:])
    log = get_logger(__file__)
    
    num_modes = (0,1)[bool(options.schema_check)] + (0,1)[bool(options.upgrade)]
    if num_modes > 1:
        parser.error('Choose only one option flag')
        
    init = {}
    for a in args:
        k,v = a.split('=')
        if k in ['connString', 'mysql_engine']:
            init[k] = v
    
    conn = ConnHandle(**init)
    s_check = SchemaCheck(conn.get_session())
    
    if options.schema_check:
        log.info('Executing schema check')
        s_check.check_schema()
    elif options.upgrade:
        log.info('Performing upgrade')
        s_check.upgrade()
    pass
    
if __name__ == '__main__':
    main()
