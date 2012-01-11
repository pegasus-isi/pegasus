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

import edu.isi.pegasus.common.credential.CredentialHandler;

import java.util.Map;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;




/**
 * A convenience class that allows us to determine the path to the user s3cfg file.
 *
 * @author Mats Rynge
 * @author Karan Vahi
 *
 * @version $Revision$
 */
public class S3CFG  extends Abstract implements CredentialHandler {

    /**
     * The name of the environment variable that specifies the path to the
     * s3cfg file.
     */
    public static final String S3CFG_FILE_VARIABLE = "S3CFG";

    /**
     * The description
     */
    private static final String DESCRIPTION = "S3 Conf File Credential Handler";


    /**
     * The default constructor.
     */
    public S3CFG(){
        super();
    }

    
    /**
     * Returns the path to s3cfg. The order of preference is as follows
     *
     * - If a s3cfg is specified in the site catalog entry that is used
     * - Else the one pointed to by the environment variable S3Cfg
     * - Else the default path to the proxy ~/.s3cfg
     *
     * @param site   the  site handle
     *
     * @return  the path to s3cfg.
     */
    public String getPath( String site ){
        SiteCatalogEntry siteEntry = mSiteStore.lookup( site );
        Map<String,String> envs = System.getenv();



        // check if one is specified in site catalog entry
        String path = ( siteEntry == null )? null :siteEntry.getEnvironmentVariable( S3CFG.S3CFG_FILE_VARIABLE );

        if( path == null){
            //check if S3Cfg is specified in the environment
            if( envs.containsKey( S3CFG.S3CFG_FILE_VARIABLE ) ){
                path = envs.get( S3CFG.S3CFG_FILE_VARIABLE );
            }
        }

        return path;
    }


    /**
     * Returns the name of the environment variable that needs to be set
     * for the job associated with the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(){
        return S3CFG.S3CFG_FILE_VARIABLE;
    }

    /**
     * Returns the description for the implementing handler
     *
     * @return  description
     */
    public String getDescription(){
        return S3CFG.DESCRIPTION;
    }
}
