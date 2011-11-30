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

package edu.isi.pegasus.common.util;

import java.io.File;
import java.util.Map;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;


/**
 * A convenience class that allows us to determine the path to the user s3cfg file.
 *
 * @author Mats Rynge
 * @version $Revision: 2572 $
 */
public class S3cfg {

    /**
     * The name of the environment variable that specifies the path to the
     * s3cfg file.
     */
    public static final String S3CFG = "S3CFG";


    /**
     * Returns the path to s3cfg file.
     *
     * @param bag the bag of inialization objects
     *
     * @return  the path to user s3cfg file.
     */
    public static final String getPathToS3cfg( PegasusBag bag ){
        SiteStore s = bag.getHandleToSiteStore();
        
        return S3cfg.getPathToS3cfg( s.lookup( "local" ), bag.getPegasusProperties() );
    }

    
    /**
     * Returns the path to s3cfg. The order of preference is as follows
     *
     * - If a s3cfg is specified in the site catalog entry that is used
     * - Else the one pointed to by the environment variable S3CFG
     * - Else the default path to the proxy ~/.s3cfg
     *
     * @param site   the  site catalog entry object.
     * @param properties  the pegasus properties object passed
     *
     * @return  the path to s3cfg.
     */
    public static final String getPathToS3cfg( SiteCatalogEntry site, PegasusProperties properties ){

        Map<String,String> envs = System.getenv();
        
        // check if one is specified in site catalog entry
        String path = ( site == null )? null :site.getEnvironmentVariable( S3cfg.S3CFG );

        if( path == null){
            //check if S3CFG is specified in the environment
            if( envs.containsKey( S3cfg.S3CFG ) ){
                path = envs.get( S3cfg.S3CFG );
            }
        }

        return path;
    }

    
    /**
     * Test program.
     * 
     * @param args
     */
    public static final void main( String[] args ){

        System.out.println( "Location of user s3cfg is " + S3cfg.getPathToS3cfg(null, null));
    }
}
