/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.poolinfo;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.parser.ConfigXmlParser;

/**
 * It gets the information about a pool by reading the pool config xml that is
 * generated from querying mds or using the static information provided by the
 * user at the submit host.
 *
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.8 $
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
