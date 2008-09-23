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


package org.griphyn.cPlanner.poolinfo;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.parser.ConfigXmlParser;

/**
 * It gets the information about a pool by reading the pool config xml that is
 * generated from querying mds or using the static information provided by the
 * user at the submit host.
 *
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class XML
    extends Abstract {

    private static XML mPoolHandle = null;

    /**
     * The private constructor that is called only once, when the Singleton is
     * invoked for the first time.
     *
     * @param poolProvider  the path to the file that contains the pool
     *                      information in the xml format.
     */
    private XML( String poolProvider ) {
        loadSingletonObjects();

        if ( poolProvider == null ) {
            throw new RuntimeException(
                " Wrong Call to the Singleton invocation of Site Catalog" );
        }

        this.mPoolProvider = poolProvider;
        ConfigXmlParser cp = new ConfigXmlParser( mPoolProvider, mProps );
        mPoolConfig = cp.getPoolConfig();
        mLogger.log( "SC Mode being used is " + this.getPoolMode(),
                     LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log( "SC File being used is " + this.mPoolProvider,
                     LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log( mPoolConfig.getSites().size() +
            " sites loaded in memory", LogManager.DEBUG_MESSAGE_LEVEL );
    }

    /**
     * The private constructor that is called to return a non singleton instance
     * of the class.
     *
     * @param poolProvider  the path to the file that contains the pool
     *                      information in the xml format.
     * @param propFileName  the name of the properties file that needs to be
     *                      picked up from PEGASUS_HOME/etc directory.If it is null,
     *                      then the default properties file should be picked up.
     *
     */
    private XML( String poolProvider, String propFileName ) {
        loadNonSingletonObjects( propFileName );
        this.mPoolProvider = poolProvider;
        ConfigXmlParser cp = new ConfigXmlParser( mPoolProvider, mProps );
        mPoolConfig = cp.getPoolConfig();
        mLogger.log( "SC Mode being used is " + this.getPoolMode(),
                     LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log( "SC File being used is " + this.mPoolProvider,
                     LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log( mPoolConfig.getSites().size() +
            " sites loaded in memory",  LogManager.DEBUG_MESSAGE_LEVEL);

    }

    /**
     * Returns a textual description about the pool mode that is
     * implemented by this class. It is purely informative.
     *
     * @return String corresponding to the description.
     */
    public String getPoolMode() {
        String st =
            "XML Site catalog";
        return st;
    }

    /**
     * The method returns a singleton instance of the derived InfoProvider class.
     *
     * @param poolProvider  the path to the file containing the pool information.
     * @param propFileName  the name of the properties file that needs to be
     *                      picked up from PEGASUS_HOME/etc directory. In the singleton
     *                      case only the default properties file is picked up.
     *
     * @return  a singleton instance of this class.
     */
    public static PoolInfoProvider singletonInstance( String poolProvider,
        String propFileName ) {

        if ( mPoolHandle == null ) {
            mPoolHandle = new XML( poolProvider );
        }
        return mPoolHandle;
    }

    /**
     * The method that returns a Non Singleton instance of the dervived
     * InfoProvider class. This method if invoked should also ensure that all
     * other internal Pegasus objects like PegasusProperties are invoked in a non
     * singleton manner.
     *
     * @param poolProvider  the path to the file containing the pool information.
     * @param propFileName  the name of the properties file that needs to be
     *                      picked up from PEGASUS_HOME/etc directory. If it is null,
     *                      then the default file should be picked up.
     *
     * @return the non singleton instance of the pool provider.
     *
     */
    public static PoolInfoProvider nonSingletonInstance( String poolProvider,
        String propFileName ) {

        mPoolHandle = new XML( poolProvider, propFileName );
        return mPoolHandle;
    }

}
