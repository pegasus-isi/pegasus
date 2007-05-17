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
package org.griphyn.common.catalog.transformation.client;

/**
 * This is a helper class which all TC client components (like tcAdd, tcDelete and tcQuery) must  extend.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.classes.SysInfo;
import org.griphyn.common.util.ProfileParser;
import org.griphyn.common.util.ProfileParserException;
import org.griphyn.common.util.Separator;

import java.util.List;
import java.util.Map;

public class Client {
    protected int trigger = 0;

    protected String lfn = null;

    protected String pfn = null;

    protected String profile = null;

    protected String type = null;

    protected String resource = null;

    protected String systemstring = null;

    protected String namespace = null;

    protected String name = null;

    protected String version = null;

    protected List profiles = null;

    protected SysInfo system = null;

    protected String file = null;

    protected LogManager mLogger = null;

    protected TransformationCatalog tc = null;

    protected boolean isxml = false;

    public Client() {
    }

    /**
     * Takes the arguments from the TCClient and stores it for acess to the other TC Client modules.
     * @param argsmap Map
     */
    public void fillArgs( Map argsmap ) {
        lfn = ( String ) argsmap.get( "lfn" );
        pfn = ( String ) argsmap.get( "pfn" );
        resource = ( String ) argsmap.get( "resource" );
        type = ( String ) argsmap.get( "type" );
        profile = ( String ) argsmap.get( "profile" );
        systemstring = ( String ) argsmap.get( "system" );
        trigger = ( ( Integer ) argsmap.get( "trigger" ) ).intValue();
        file = ( String ) argsmap.get( "file" );
        isxml = ( ( Boolean ) argsmap.get( "isxml" ) ).booleanValue();
        if ( lfn != null ) {
            String[] logicalname = Separator.split( lfn );
            namespace = logicalname[ 0 ];
            name = logicalname[ 1 ];
            version = logicalname[ 2 ];
        }
        if ( profile != null ) {
            try {
                profiles = ProfileParser.parse( profile );
            } catch ( ProfileParserException ppe ) {
                mLogger.log( "Parsing profiles " + ppe.getMessage() +
                    "at position " + ppe.getPosition(), ppe,
                    LogManager.ERROR_MESSAGE_LEVEL );
            }
        }
        if ( systemstring == null ) {
            system = new SysInfo( systemstring );
        }
    }

}
