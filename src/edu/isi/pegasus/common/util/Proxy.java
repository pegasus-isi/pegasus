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

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.common.PegasusProperties;

import org.globus.common.CoGProperties;

import java.util.Map;


/**
 * A convenice class that allows us to determine the path to the user proxy.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Proxy {


    /**
     * The name of the environment variable that specifies the path to the
     * proxy.
     */
    public static final String X509_USER_PROXY_KEY = "X509_USER_PROXY";



    /**
     * Returns the path to user proxy.
     *
     * @param bag the bag of inialization objects
     *
     * @return  the path to user proxy.
     */
    public static final String getPathToUserProxy( PegasusBag bag ){
        SiteStore s = bag.getHandleToSiteStore();
        return Proxy.getPathToUserProxy( s.lookup( "local" ), bag.getPegasusProperties() );
    }

    /**
     * Returns the path to user proxy. The order of preference is as follows
     *
     * - If a proxy is specified in the site catalog entry that is used
     * - Else the one pointed to by the environment variable X509_USER_PROXY
     * - Else the default path to the proxy in /tmp is created as determined by
     *     CoGProperties.getDefault().getProxyFile()
     *
     * @param site   the  site catalog entry object.
     * @param properties  the pegasus properties object passed
     *
     * @return  the path to user proxy.
     */
    public static final String getPathToUserProxy( SiteCatalogEntry site, PegasusProperties properties ){
        //check if one is specified in site catalog entry
        String proxy = ( site == null )? null :site.getEnvironmentVariable( Proxy.X509_USER_PROXY_KEY);

        if( proxy == null){
            //check if X509_USER_PROXY is specified in the environment
            Map<String,String> envs = System.getenv();
            if( envs.containsKey( Proxy.X509_USER_PROXY_KEY ) ){
                proxy = envs.get( Proxy.X509_USER_PROXY_KEY );
            }
        }


        if( proxy == null ){
            //construct default path to user proxy in /tmp
            proxy = CoGProperties.getDefault().getProxyFile();
        }


        //overload from the properties file
        /*
        ENV env = new ENV();
        env.checkKeyInNS( mProps,"local" );
        proxy = env.containsKey( ENV.X509_USER_PROXY_KEY )?
                (String)env.get( ENV.X509_USER_PROXY_KEY ):
                proxy;
        */

        return proxy;
    }

    /**
     * Test program.
     * 
     * @param args
     */
    public static final void main( String[] args ){

        System.out.println( "User proxy is " + Proxy.getPathToUserProxy(null, null));
    }
}
