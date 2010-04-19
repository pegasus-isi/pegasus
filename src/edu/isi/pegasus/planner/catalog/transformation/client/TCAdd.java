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

package edu.isi.pegasus.planner.catalog.transformation.client;

/**
 * This is a TCClient class which handles the Add Operations.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import org.griphyn.common.util.ProfileParser;

import org.griphyn.common.util.ProfileParserException;
import org.griphyn.common.util.Separator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class TCAdd
    extends Client {

    public TCAdd( TransformationCatalog tc, LogManager mLogger, Map argsmap ) {
        this.fillArgs( argsmap );
        this.tc = tc;
        this.mLogger = mLogger;
    }

    public void doAdds() {
        try {
            //SWitch for what triggers are defined.
            switch ( trigger ) {
                case 0: //normal tc entry
                    if ( this.addEntry() ) {
                        mLogger.log( "Added tc entry sucessfully",
                                    LogManager.INFO_MESSAGE_LEVEL );
                    } else {
                        mLogger.log( "Unable to add tc entry",
                                    LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    break;
                case 1: //bulk mode tc entry
                    if ( this.addBulk() ) {
                        mLogger.log( "Added bulk tc entries sucessfully",
                                    LogManager.INFO_MESSAGE_LEVEL );
                    } else {
                        mLogger.log( "Unable to add bulk tc entries",
                                    LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    break;
                case 18: //lfn profile additions
                    if ( lfn == null ) {
                        mLogger.log(
                            "The logical transformation cannot be null.  " ,
                            LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                            "Please try tc-client --help or man tc-client for more information." ,
                            LogManager.ERROR_MESSAGE_LEVEL);
                        System.exit( 1 );

                    }
                    mLogger.log( "Trying to add profiles for lfn " +
                        lfn +
                        " " + profiles, LogManager.DEBUG_MESSAGE_LEVEL);
                    if ( tc.addTCLfnProfile( namespace, name, version,
                        profiles ) ) {
                        mLogger.log( "Added lfn profiles sucessfully",
                                    LogManager.INFO_MESSAGE_LEVEL );
                    } else {
                        mLogger.log( "Unable to add LFN profiles",
                                     LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    break;
                case 19: //bulk lfn profile additions
                    break;
                case 20: //pfn profile additions
                    if ( resource == null || type == null || pfn == null ) {
                        mLogger.log(
                            "The resourceid or physical name or type cannot be null.",
                           LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "Please try tc-client --help or man tc-client for more information.",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    mLogger.log( "Trying to add profiles for pfn " +
                        pfn +
                        " " + resource + " " + type + " " +
                        profiles,LogManager.DEBUG_MESSAGE_LEVEL);
                    if ( tc.addTCPfnProfile( pfn, TCType.fromString( type ),
                        resource, profiles ) ) {
                        mLogger.log( "Added pfn profiles sucessfully",
                                    LogManager.INFO_MESSAGE_LEVEL );
                    } else {
                        mLogger.log( "Unable to add PFN profiles",
                                     LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    break;
                case 21: //bulk pfn profile additions
                    break;
                default:
                    mLogger.log( "Wrong trigger invoked in TC ADD" ,
                                 LogManager.ERROR_MESSAGE_LEVEL);
                    mLogger.log(
                        "Check tc-client --help or man tc-client for correct usage." ,
                        LogManager.FATAL_MESSAGE_LEVEL);
                    System.exit( 1 );
            }
        } catch ( Exception e ) {
            mLogger.log(
                "Unable to add entry to TC", e ,LogManager.ERROR_MESSAGE_LEVEL);
            e.printStackTrace();
            System.exit( 1 );
        }

    }

    /**
     * Adds a single entry into the TC.
     * @throws Exception
     * @return boolean
     */
    private boolean addEntry() throws Exception {
        if ( lfn == null || pfn == null || resource == null ) {
            System.out.println(
                "Error : Please enter atleast the lfn, pfn and resource you want to add" );
            System.out.println( "See tc-client --help for more information." );
            System.exit( 1 );
            return false;
        }
        TCType t = ( type == null ) ? TCType.INSTALLED :
            TCType.fromString( type );
        mLogger.log( "Trying to add entry in TC with " + namespace +
            "::" +
            name + ":" + version +
            " " + pfn + " " + t + " " + resource + " " + system +
            " " + profiles, LogManager.DEBUG_MESSAGE_LEVEL);
        SysInfo s = ( system == null ) ? new SysInfo() : system;
        return ( tc.addTCEntry( namespace, name, version,
            pfn, t,
            resource, null, profiles, s ) ) ? true : false;
    }

    /**
     * Adds multiple entries into the TC.  Calls the above api multiple times.
     * @return boolean
     */
    private boolean addBulk() {
        BufferedReader buf = null;
        int linecount = 0;
        int count = 0;
        TCType ttype = null;
        try {
            String line = null;
            buf = new BufferedReader( new FileReader( file ) );
            while ( ( line = buf.readLine() ) != null ) {
                linecount++;
                if ( ! ( line.startsWith( "#" ) ||
                    line.trim().equalsIgnoreCase( "" ) ) ) {
                    lfn = null;
                    namespace = null;
                    name = null;
                    version = null;
                    type = null;
                    profiles = null;
                    pfn = null;
                    resource = null;
                    systemstring = null;
                    profile = null;
                    String[] tokens = line.split( "[ \t]+", 6 );
                    for ( int i = 0; i < tokens.length; i++ ) {
                        switch ( i ) {
                            case 0: //poolname
                                resource = tokens[ i ];
                                break;
                            case 1: //logical transformation name
                                lfn = tokens[ i ];
                                String[] s = Separator.split( lfn );
                                namespace = s[ 0 ];
                                name = s[ 1 ];
                                version = s[ 2 ];
                                break;
                            case 2: //pfn
                                pfn = tokens[ i ];
                                break;
                            case 3: //type
                                ttype = ( tokens[ i ].equalsIgnoreCase( "null" ) ) ?
                                    TCType.INSTALLED :
                                    TCType.fromString( tokens[ i ] );
                                break;
                            case 4: //systeminfo
                                system = ( tokens[ i ].equalsIgnoreCase( "null" ) ) ?
                                    new SysInfo( null ) :
                                    new SysInfo( tokens[ i ] );
                                systemstring = system.toString();
                                break;
                            case 5: //profile string
                                profile = ( tokens[ i ].equalsIgnoreCase(
                                    "null" ) ) ? null :
                                    tokens[ i ];
                                break;
                            default:
                                mLogger.log( "Line " + linecount +
                                    " : Humm no need to be in default",
                                   LogManager.ERROR_MESSAGE_LEVEL );
                        } //end of switch
                    } //end of for loop
                    try {
                    profiles = ProfileParser.parse( profile );
                }catch (ProfileParserException ppe) {
                    mLogger.log( "Unable to parse profiles on line "+
                                 linecount+" "+ppe.getMessage()+ "at position "+
                                 ppe.getPosition(), ppe ,
                                 LogManager.ERROR_MESSAGE_LEVEL);
                }
                    if ( !tc.addTCEntry( namespace, name, version, pfn, ttype,
                        resource, null, profiles, system ) ) {
                        mLogger.log(
                            "Unable to bulk entries into tc on line " +
                            linecount ,LogManager.ERROR_MESSAGE_LEVEL);
                        return false;
                    }
                    count++;
                    // this.addEntry();
                } //end of if "#"
            } //end of while line
            mLogger.log( "Added " + count + " entries to the TC" ,
                         LogManager.INFO_MESSAGE_LEVEL);
            buf.close();
            return true;
        } catch ( FileNotFoundException ex ) {
            mLogger.log( "The tc text file " + file +
                " was not found", ex ,LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        } catch ( IOException e ) {
            mLogger.log( "Unable to open the file " +
                         file, e,LogManager.ERROR_MESSAGE_LEVEL );
            return false;
        } catch ( Exception e ) {
            mLogger.log(
                "Unable to add bulk entries into tc on line " + linecount,
                e ,LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
    }
}
