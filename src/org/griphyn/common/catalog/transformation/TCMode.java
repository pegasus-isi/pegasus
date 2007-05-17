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

package org.griphyn.common.catalog.transformation;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.util.DynamicLoader;

/**
 *  This class defines all the constants
 *     referring to the various interfaces
 *     to the transformation catalog, and
 *     used by the Concrete Planner.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TCMode {

    /**
     * Constants for backward compatibility.
     */

    public static final String SINGLE_READ = "single";

    public static final String MULTIPLE_READ = "multiple";

    public static final String OLDFILE_TC_CLASS = "OldFile";

    public static final String DEFAULT_TC_CLASS = "File";
    /**
     * Default PACKAGE PATH for the TC implementing classes
     */
    public static final String PACKAGE_NAME =
        "org.griphyn.common.catalog.transformation.";

    private static LogManager mLogger = LogManager.getInstance();

    //add your constants here.

    /**
     * This method just checks and gives the correct classname if a user provides the classname in a different case.
     * @param tcmode String
     * @return String
     */
    private static String getImplementingClass( String tcmode ) {

        if ( tcmode.trim().equalsIgnoreCase( SINGLE_READ ) ||
            tcmode.trim().equalsIgnoreCase( MULTIPLE_READ ) ) {
            return OLDFILE_TC_CLASS;
        } else {
            //no match to any predefined constant
            //assume that the value of readMode is the
            //name of the implementing class
            return tcmode;
        }
    }

    /**
     * The overloaded method which is to be used internally in Pegasus.
     *
     * @return TCMechanism
     */
    public static TransformationCatalog loadInstance() {
        PegasusProperties mProps = PegasusProperties.getInstance();
        TransformationCatalog tc = null;
        String tcClass = getImplementingClass( mProps.getTCMode() );

        //if (tcClass.equals(FILE_TC_CLASS)) {
        //  String[] args = {mProps.getTCPath()};
        // return loadInstance(tcClass, args);
        // } else {
        String[] args = new String[0 ];
        tc=loadInstance( tcClass, args );
        if(tc==null) {
            mLogger.log(
                "Unable to load TC",LogManager.FATAL_MESSAGE_LEVEL);
                System.exit(1);
        }
   return tc;
        //  }
    }

    /**
     * Loads the appropriate TC implementing Class with the given arguments.
     * @param tcClass String
     * @param args String[]
     * @return TCMechanism
     */
    public static TransformationCatalog loadInstance( String tcClass,
        Object[] args ) {

        TransformationCatalog tc = null;
        String methodName = "getInstance";
        //get the complete name including
        //the package if the package name not
        //specified
        if ( tcClass.indexOf( "." ) == -1 ) {
            tcClass = PACKAGE_NAME + tcClass;
        }

        DynamicLoader d = new DynamicLoader( tcClass );

        try {
            tc = ( TransformationCatalog ) d.static_method( methodName, args );

            //This identifies the signature for
            //the method

        } catch ( Exception e ) {
            mLogger.log( d.convertException( e ), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit( 1 );
        }
        return tc;

    }

}
