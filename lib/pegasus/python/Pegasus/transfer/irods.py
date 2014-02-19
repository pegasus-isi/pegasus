##
#  Copyright 2007-2014 University Of Southern California
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

__author__ = "Mats Rynge <rynge@isi.edu>"

import os
from Pegasus.transfer.utils import *

class IRods(object):
    
    ticket = None
 
    def login(self, sitename=""):
        """
        log in to irods by using the iinit command - if the file already exists,
        we are already logged in
        """
        key = "irodsEnvFile_" + sitename
        if key in os.environ:
            os.environ["irodsEnvFile"] = os.environ[key]
   
        if self.ticket != None:
            return
   
        f = os.environ['irodsAuthFileName']
        if os.path.exists(f):
            return
       
        # read password from env file
        if not "irodsEnvFile" in os.environ:
            raise RuntimeError("Missing irodsEnvFile - unable to do irods "
                              + " transfers")
       
        check_cred_fs_permissions(os.environ["irodsEnvFile"])
               
        password = None
        self.ticket = None
        h = open(os.environ['irodsEnvFile'], 'r')
        for line in h:
            items = line.split(" ", 2)
            if items[0].lower() == "irodspassword":
                password = items[1].strip(" \t'\"\r\n")
            if items[0].lower() == "irodsticket":
                self.ticket = items[1].strip(" \t'\"\r\n")
        h.close()
        if password is None and self.ticket is None:
            raise RuntimeError("No irodsTicket or irodsPassword" +
                               " specified in irods env file")
       
        if password is not None:
            h = open(".irodsAc", "w")
            h.write(password + "\n")
            h.close()
            cmd = "cat .irodsAc | iinit"
            myexec(cmd, 5*60, True)
            os.unlink(".irodsAc")
            check_cred_fs_permissions(os.environ['irodsAuthFileName'])

