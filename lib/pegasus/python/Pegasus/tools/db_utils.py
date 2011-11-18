"""
db_utils.py: Provides database related functions used by several monitoring tools
"""

##
#  Copyright 2007-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2012 $

import os
import logging

from Pegasus.tools import properties
from Pegasus.tools import utils

# Get logger object (initialized elsewhere)
logger = logging.getLogger()

def get_db_url_wf_uuid(submit_dir, config_properties):
    """
    Utility method for returning the db_url and wf_uuid given the submit_dir and pegasus properties file.
    @submit_dir submit directory path
    @config_properties config properties file path
    """

    # Getting values from braindump file
    top_level_wf_params = utils.slurp_braindb(submit_dir)
    top_level_prop_file = None

    # Return if we cannot parse the braindump.txt file
    if not top_level_wf_params:
        logger.error("Unable to process braindump.txt ")
        return None, None

    # Get wf_uuid for this workflow
    wf_uuid = None
    if (top_level_wf_params.has_key('wf_uuid')):
        wf_uuid = top_level_wf_params['wf_uuid']
    else:
        logger.error("workflow id cannot be found in the braindump.txt ")
        return None, None

    # Get the location of the properties file from braindump

    # Get properties tag from braindump
    if "properties" in top_level_wf_params:
        top_level_prop_file = top_level_wf_params["properties"]
        # Create the full path by using the submit_dir key from braindump
        if "submit_dir" in top_level_wf_params:
            top_level_prop_file = os.path.join(top_level_wf_params["submit_dir"], top_level_prop_file)

    # Parse, and process properties
    props = properties.Properties()
    props.new(config_file=config_properties, rundir_propfile=top_level_prop_file)

    # Ok, now figure out the database URL
    output_db_url = None

    if props.property('pegasus.monitord.output') is not None:
        output_db_url = props.property('pegasus.monitord.output')

        # Return, if not using sqlite or mysql
        if not (output_db_url.startswith("mysql:") or output_db_url.startswith("sqlite:")):
            logger.error("Unable to find database file from the properties file ")
            return None, None
    else:
        # Ok, the default case is a .stampede.db file with the dag name as base
        dag_file_name = ''
        if (top_level_wf_params.has_key('dag')):
            dag_file_name = top_level_wf_params['dag']
        else:
            logger.error("dag file name cannot be found in the braindump.txt")
            return None, None

        # Create the sqllite db url
        output_db_file = submit_dir + "/" + dag_file_name[:dag_file_name.find(".dag")] + ".stampede.db"
        output_db_url = "sqlite:///" + output_db_file
        if not os.path.isfile(output_db_file):
            logger.error("Unable to find database file in " + submit_dir)
            return None, None

    # Ok, all done!
    return output_db_url, wf_uuid

if __name__ == "__main__":
    pass
