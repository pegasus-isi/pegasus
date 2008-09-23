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

package org.griphyn.common.catalog.transformation.client;


import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.common.catalog.TransformationCatalog;

import org.griphyn.common.classes.TCType;

import java.util.Map;

/**
 * This is a TCClient class which handles the Delete Operations.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
public class TCDelete
    extends Client {

    public TCDelete( TransformationCatalog tc, LogManager mLogger, Map argsmap ) {
        this.fillArgs( argsmap );
        this.tc = tc;
        this.mLogger = mLogger;
    }

    public void doDelete() {
        //SWitch for what triggers are defined.
        try {
            switch ( trigger ) {

                case 2: //delete TC by logical name
                    if ( name == null ) {
                        mLogger.log(
                            "You need to provide the logical name by which you want to delete",
                           LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC by logical name " + lfn +
                            " on resource " +
                            ( ( resource == null ) ? "ALL" : resource ) +
                            " and type " + ( ( type == null ) ? "ALL" : type ),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCbyLogicalName( namespace, name, version,
                            resource,
                            ( ( type == null ) ? null : TCType.fromString( type ) ) ) ) {
                            mLogger.log(
                                "Deleted the TC entries by logical name " + lfn +
                                " on resource " +
                                ( ( resource == null ) ? "ALL" : resource ) +
                                " and type " + ( ( type == null ) ? null : type ),
                               LogManager.INFO_MESSAGE_LEVEL );
                        } else {
                            mLogger.log(
                                "Unabelt to detele TC by logical name " +
                                lfn + " on resource " +
                                ( ( resource == null ) ? null : resource ) +
                                " and type " + ( ( type == null ) ? "ALL" :
                                type ) ,LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit( 1 );
                        }
                    }
                    break;

                case 4: //delete TC by physical name
                    if ( pfn == null || name == null ) {
                        mLogger.log( "You need to provide the pfn and logical " +
                                     "name by which you want to delete" ,
                                     LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC by physicalname " + pfn +
                            " and logical name " + lfn + " on resource " +
                            ( ( resource == null ) ? "ALL" : resource ) +
                            " and type " + ( ( type == null ) ? "ALL" : type ),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCbyPhysicalName( pfn, namespace, name,
                            version, resource,
                            ( ( type == null ) ? null : TCType.fromString( type ) ) ) ) {
                            mLogger.log(
                                "Deleted the TC entries by physicalname " + pfn +
                                " and logical name " + lfn + " on resource " +
                                ( ( resource == null ) ? "ALL" : resource ) +
                                " and type " + ( ( type == null ) ? "ALL" :
                                type ) ,LogManager.INFO_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                "Unable to delete TC by physicalname " +
                                pfn + " and logical name " + lfn +
                                " on resource " +
                                ( ( resource == null ) ? "ALL" : resource ) +
                                " and type " + ( ( type == null ) ? "ALL" :
                                type ),LogManager.FATAL_MESSAGE_LEVEL );
                            System.exit( 1 );
                        }
                    }
                    break;
                case 8: //delete TC by resource
                    if ( resource == null ) {
                        mLogger.log(
                            "You need to provide the resourceid by which you want to delete",
                           LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC by resourceid " + resource,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCbyResourceId( resource ) ) {
                            mLogger.log(
                                "Deleted the TC entries by resourceid " +
                                resource,LogManager.INFO_MESSAGE_LEVEL );
                        } else {
                            mLogger.log(
                                "Unable to delete TC by resourceid" ,
                                LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit( 1 );
                        }
                    }
                    break;
                case 18: //delete TC lfnprofiles
                    if ( name == null ) {
                        mLogger.log( "You need to provide the logical transformation by " +
                                     "which you want to delete the profiles",
                                    LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details " ,
                            LogManager.FATAL_MESSAGE_LEVEL);
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC LFN profiles for LFN " +
                            lfn,LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCLfnProfile( namespace, name, version,
                            profiles ) ) {
                            mLogger.log(
                                "Deleted the TC LFN profile entries for LFN " +
                                lfn , LogManager.INFO_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                "Unable to delete the TC LFN profiles",
                                LogManager.FATAL_MESSAGE_LEVEL );
                            System.exit( 1 );
                        }
                    }
                    break;
                case 20: //delete TC pfnprofiles
                    if ( pfn == null || resource == null || type == null ) {
                        mLogger.log( " You need to provide the physical transformation, " +
                                     " resource and type by which you want to delete the profiles",
                                     LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC PFN profiles for PFN " +
                            pfn + " type " + type + " resource " + resource,
                           LogManager.DEBUG_MESSAGE_LEVEL );
                        if ( tc.deleteTCPfnProfile( pfn,
                            TCType.fromString( type ),
                            resource, profiles ) ) {
                            mLogger.log(
                                "Delete the TC PFN profile entries for PFN " +
                                pfn + " type " + type + " resource " + resource,
                               LogManager.DEBUG_MESSAGE_LEVEL );
                        } else {
                            mLogger.log(
                                "Unable to delete the TC PFN profiles",
                               LogManager.FATAL_MESSAGE_LEVEL );
                            System.exit( 1 );
                        }
                    }
                    break;
                case 32: //delete by TC type
                    if ( type == null ) {
                        mLogger.log( "You need to provide the transformation type by " +
                                     "which you want to delete the TC." ,
                                     LogManager.ERROR_MESSAGE_LEVEL);
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log ("Trying to delete the TC by Type " +
                            type + "and resource " +
                            ( ( resource == null ) ? "ALL" :
                            resource ), LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCbyType( TCType.fromString( type ),
                            resource ) ) {
                            mLogger.log(
                                "Deleted the TC entries for Type " + type +
                                " resource " +
                                ( ( resource == null ) ? "ALL" : resource ),
                               LogManager.INFO_MESSAGE_LEVEL );
                        } else {
                            mLogger.log(
                                "Unable to delete the TC by type" ,
                                LogManager.FATAL_MESSAGE_LEVEL);
                            System.exit( 1 );
                        }
                    }
                    break;

                case 64: //delete the  TC by sysinfo.
                    if ( system == null ) {
                        mLogger.log( "You need to provide the transformation sysinfo "+
                                     "by which you want to delete the TC.",
                                    LogManager.ERROR_MESSAGE_LEVEL );
                        mLogger.log(
                            "See tc-client --help or man tc-client for more details ",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    } else {
                        mLogger.log(
                            "Trying to delete the TC by SysInfo " +
                            systemstring, LogManager.DEBUG_MESSAGE_LEVEL);
                        if ( tc.deleteTCbySysInfo( system ) ) {
                            mLogger.log(
                                "Deleted the TC entries for SysInfo " +
                                systemstring ,LogManager.INFO_MESSAGE_LEVEL);
                        } else {
                            mLogger.log(
                                "Unable to delete the TC by SysInfo",
                               LogManager.FATAL_MESSAGE_LEVEL );
                            System.exit( 1 );
                        }
                    }

                    break;
                case 127: //delete entire TC. whoopa.
                    mLogger.log( "Trying to delete the entire TC ",
                                 LogManager.DEBUG_MESSAGE_LEVEL);
                    if ( tc.deleteTC() ) {
                        mLogger.log( "Deleted the entire tc succesfully",
                                    LogManager.INFO_MESSAGE_LEVEL );
                    } else {
                        mLogger.log(
                            "Error while deleting entire TC",
                           LogManager.FATAL_MESSAGE_LEVEL );
                        System.exit( 1 );
                    }
                    break;

                default:
                    mLogger.log(
                        "Wrong trigger invoked in TC Delete. Try tc-client --help for a detailed help." +
                        trigger, LogManager.FATAL_MESSAGE_LEVEL );
                    System.exit( 1 );
            }
        } catch ( Exception e ) {
            mLogger.log( "Unable to do delete operation", e,
                        LogManager.FATAL_MESSAGE_LEVEL );
            e.printStackTrace();
            System.exit( 1 );
        }
    }

}
