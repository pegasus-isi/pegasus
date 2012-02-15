/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.common.credential.impl;

import java.io.File;
import java.util.Map;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;




/**
 * A convenience class that allows us to determine the path to the user ssh private key file.
 *
 * @author Mats Rynge
 * @author Karan Vahi
 *
 * @version $Revision: 4817 $
 */
public class Ssh extends Abstract implements CredentialHandler {

    /**
     * The name of the environment variable that specifies the path to the
     * s3cfg file.
     */
    public static final String SSH_PRIVATE_KEY_VARIABLE = "SSH_PRIVATE_KEY";

    /**
     * The description
     */
    private static final String DESCRIPTION = "SSH private key Credential Handler";


    /**
     * The default constructor.
     */
    public Ssh(){
        super();
    }

    
    /**
     * Returns the path to ssh private key. The key has to be specifically listed in the environment
     * @param site   the  site handle
     *
     * @return  the path to s3cfg.
     */
    public String getPath( String site ){
        SiteCatalogEntry siteEntry = mSiteStore.lookup( site );
        Map<String,String> envs = System.getenv();

        // check if one is specified in site catalog entry
        String path = ( siteEntry == null )? null :siteEntry.getEnvironmentVariable( Ssh.SSH_PRIVATE_KEY_VARIABLE);

        if( path == null){
            //check if specified in the environment
            if( envs.containsKey( Ssh.SSH_PRIVATE_KEY_VARIABLE ) ){
                path = envs.get( Ssh.SSH_PRIVATE_KEY_VARIABLE );
            }
        }

        return path;
    }

    
    /**
     * returns the basename of the path to the local credential
     */
    public String getBaseName() {
        File path = new File(this.getPath());
        return path.getName();
    }


    /**
     * Returns the name of the environment variable that needs to be set
     * for the job associated with the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(){
        return Ssh.SSH_PRIVATE_KEY_VARIABLE;
    }

    /**
     * Returns the description for the implementing handler
     *
     * @return  description
     */
    public String getDescription(){
        return Ssh.DESCRIPTION;
    }
}
