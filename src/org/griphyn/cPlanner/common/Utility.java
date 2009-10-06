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

package org.griphyn.cPlanner.common;

import java.net.URL;

import java.util.StringTokenizer;


/**
 * A utility class that contains a few common utility/helper functions used in
 * Pegasus. At present they are preliminary URL decomposition functions.
 *
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public  class Utility {

    /**
     * This returns the host name of the server, given a url prefix.
     *
     * @param urlPrefix  the urlPrefix of the server.
     * @return String
     */
    public static String getHostName(String urlPrefix) {
        StringTokenizer st = new StringTokenizer(urlPrefix);
        String hostName = new String();
        String token = new String();
        int count = 0;

        while (st.hasMoreTokens()) {
            token = st.nextToken("/");
            count++;
            if (count == 2) {
                hostName = token.trim();
                //System.out.println("host name " + hostName);
                return hostName;
            }

        }
        return hostName;

    }

    /**
     * Prunes the url prefix to ensure that only the url prefix as wanted
     * by Pegasus goes through. This is due to the different manner in which
     * url prefix was used earlier.
     *
     * For e.g the function when passed a url
     * gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output/
     * returns url gsiftp://dataserver.phys.uwm.edu.
     *
     * @param url  the url prefix.
     * @return String
     */
    public static String pruneURLPrefix(String url) {
        String hostName = getHostName(url);
        url = url.substring(0,
                            url.lastIndexOf(hostName) +
                            hostName.length()).trim();

        return url;

    }

    /**
     * It returns the absolute path of the url. The absolute path is the
     * directory path in the URL. In the GVDS lingo, it refers to the mount
     * points too.
     *
     * @param url String
     * @return String
     */
    public static String getAbsolutePath( String url ) {
        String hostName = null;
        URL u = null;

        //try using the java url class to get mount point
        try {
            u = new URL( url );
        } catch ( Exception e ) {
            //the url seems to be malformed. could be the gsiftp trigger
            u = null;
            //use our own method to get the url
            hostName = getHostName( url );
        }
        return ( u == null ) ?
            //try to do some inhouse magic
            url.substring( url.indexOf( hostName ) +
            hostName.length() ).trim()
            :
            //malformed execption caught. most probably due to
            //invalid protocol/schema
            u.getPath();
    }



}
