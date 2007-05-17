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

import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;
import org.griphyn.common.classes.SysInfo;
import org.griphyn.common.classes.TCType;
import org.griphyn.common.util.ProfileParser;
import org.griphyn.common.util.Separator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Date;

/**
 *  A singleton implementation to get
 * a handle to the Old File Transformation Catalog.
 *
 * @author Gaurang Mehta
 * @version $Revision: 1.11 $
 */

public class OldFile
    implements TransformationCatalog {

    /**
     * The LogManager object which is
     * used to log all the messages.
     * It's values are set in the CPlanner class.
     */
    protected LogManager mLogger;

    /**
     * the text file which for the
     * time being serves as the
     * transformation catalog
     */
    protected String mTCFile;

    /**
     * the List containing
     * the user specified list
     * of pools on which he wants
     * the dag to run
     *
     */
    protected List mvExecPools;

    /**
     * The Tree Map which stores
     * the contents of the file.
     * The key would be
     * poolname_transformationname
     */
    private Map mTreeMap;

    private PegasusProperties mprops;

    /**
     * The singleton object which
     * is returned
     */
    private static OldFile mTCFileHandle = null;

    private String tcFile;

    /**
     * returns an instance to the
     * singleton object
     * @return TransformationCatalog
     */
    public static TransformationCatalog getInstance() {
        if ( mTCFileHandle == null ) {
            mTCFileHandle = new OldFile();
        }
        return mTCFileHandle;
    }

    public boolean connect( java.util.Properties props ) {
        //not implemented
        return true;
    }

    /**
     * the private constructor
     * initialises the file handles
     * to tc file
     *
     */
    private OldFile() {
        mprops = PegasusProperties.nonSingletonInstance();
        this.tcFile = mprops.getTCPath();
        mLogger = LogManager.getInstance();
        mTreeMap = new TreeMap();
        mLogger.log( "TC Mode being used is " + this.getTCMode(),
            LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "TC File being used is " + tcFile,
            LogManager.DEBUG_MESSAGE_LEVEL );
        if ( tcFile == null ) {
            mLogger.log(
                "The File to be used as TC should be defined with the property vds.tc.file",
                LogManager.FATAL_MESSAGE_LEVEL );
            System.exit( 1 );
        }
        populateTC();

    }

    /**
     * returns a textual description of the
     * transformation mode
     * @return String
     */
    public String getTCMode() {
        String st = "OLD FILE TC Mode";
        return st;
    }

    public List getTCEntries( String namespace, String name, String version,
        List resourceids, TCType type ) throws Exception {
        List results = null;
        if ( resourceids != null ) {
            for ( Iterator i = resourceids.iterator(); i.hasNext(); ) {
                List tempresults = getTCEntries( namespace, name, version,
                    ( String ) i.next(), type );
                if ( tempresults != null ) {
                    if ( results == null ) {
                        results = new ArrayList();
                    }
                    results.addAll( tempresults );
                }
            }
        } else {
            List tempresults = getTCEntries( namespace, name, version, ( String )null,
                type );
            if ( tempresults != null ) {
                results = new ArrayList( tempresults.size() );
                results.addAll( tempresults );
            }

        }
        return results;
    }

    public List getTCEntries( String namespace, String name, String version,
        String resourceid, TCType type ) throws Exception {
        List results = null;
        String lfn = Separator.combine( namespace, name, version );
        mLogger.log( "Trying to get TCEntries for " +
            lfn + " on resource " + ( ( resourceid == null ) ? "ALL" :
            resourceid ) +
            " of type " + ( ( type == null ) ? "ALL" : type.toString() ),
            LogManager.DEBUG_MESSAGE_LEVEL );
        if ( resourceid != null ) {
            if ( mTreeMap.containsKey( resourceid ) ) {
                Map lfnMap = ( Map ) mTreeMap.get( resourceid );
                if ( lfnMap.containsKey( lfn ) ) {
                    List l = ( List ) lfnMap.get( lfn );
                    if ( type != null && l != null ) {
                        for ( Iterator i = l.iterator(); i.hasNext(); ) {
                            TransformationCatalogEntry tc = (
                                TransformationCatalogEntry ) i.next();
                            if ( tc.getType().equals( type ) ) {
                                if ( results == null ) {
                                    results = new ArrayList();
                                }
                                results.add( tc );
                            }
                        }
                    } else {
                        results = l;
                    }
                }
            }
        } else {
            //since resourceid is null return entries for all sites
            if ( !mTreeMap.isEmpty() ) {

                for ( Iterator j = mTreeMap.values().iterator(); j.hasNext(); ) {
                    //check all maps for the executable.
                    Map lfnMap = ( Map ) j.next();
                    if ( lfnMap.containsKey( lfn ) ) {
                        List l = ( List ) lfnMap.get( lfn );
                        if ( type != null && l != null ) {
                            for ( Iterator i = l.iterator(); i.hasNext(); ) {
                                TransformationCatalogEntry tc = (
                                    TransformationCatalogEntry ) i.next();
                                if ( tc.getType().equals( type ) ) {
                                    if ( results == null ) {
                                        results = new ArrayList();
                                    }
                                    results.add( tc );
                                }
                            }
                        } else {
                            //if the list returned is not empty keep adding to the result list.
                            if ( l != null ) {
                                if ( results == null ) {
                                    results = new ArrayList();
                                }
                                results.addAll( l );
                            }
                        }
                    }
                }
            }
        }
        return results;
    }

    /**
     * Get the list of Resource ID's where a particular transformation may reside.
     *
     * @param   namespace String The namespace of the transformation to search for.
     * @param   name      String The name of the transformation to search for.
     * @param   version   String The version of the transformation to search for.
     * @param   type      TCType The type of the transformation to search for.<BR>
     *                    (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                     If NULL it returns all types.
     * @return  List      Returns a list of Resource Id's as strings.
     * @throws  Exception NotImplementedException if not implemented
     */
    public List getTCResourceIds( String namespace, String name, String version,
        TCType type ) throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    /**
     * Get the list of PhysicalNames for a particular transformation on a site/sites for a particular type/types;
     * @param  namespace  String The namespace of the transformation to search for.
     * @param  name       String The name of the transformation to search for.
     * @param  version    String The version of the transformation to search for.
     * @param  resourceid String The id of the resource on which you want to search. <BR>
     *                    If <B>NULL</B> then returns entries on all resources
     * @param  type       TCType The type of the transformation to search for. <BR>
     *                        (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                        If <B>NULL</B> then returns entries of all types.
     * @throws Exception  NotImplementedException if not implemented.
     * @return List       Returns a list of physical names as strings.
     */
    public List getTCPhysicalNames( String namespace, String name,
        String version,
        String resourceid, TCType type ) throws
        Exception {
        List results = null;
        List lfnMap = new ArrayList();
        int count[] = {0, 0, 0};
        if ( resourceid == null ) {
            lfnMap.addAll( mTreeMap.values() );
        } else {
            if ( mTreeMap.containsKey( resourceid ) ) {
                lfnMap.add( mTreeMap.get( resourceid ) );
            } else {
                return null;
            }
        }

        for ( Iterator i = lfnMap.iterator(); i.hasNext(); ) {
            Map lMap = ( Map ) i.next();
            if ( lMap.containsKey( Separator.combine( namespace, name, version ) ) ) {
                for ( Iterator j = ( ( List ) lMap.get( Separator.combine(
                    namespace,
                    name, version ) ) ).iterator(); j.hasNext(); ) {
                    TransformationCatalogEntry entry = (
                        TransformationCatalogEntry ) j.next();
                    if ( type != null ) {
                        if ( !entry.getType().equals( type ) ) {
                            break;
                        }
                    }
                    String[] s = {entry.getResourceId(),
                        entry.getPhysicalTransformation(),
                        entry.getType().toString(),
                        entry.getSysInfo().toString()};
                    columnLength( s, count );
                    if ( results == null ) {
                        results = new ArrayList();
                    }
                    results.add( s );
                }
            }
        }
        if ( results != null ) {
            results.add( count );
        }
        return results;
    }

    public List getTCLogicalNames( String resourceid, TCType type ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public List getTCLfnProfiles( String namespace, String name, String version ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public List getTCPfnProfiles( String pfn, String resourceid, TCType type ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    //
    //ADDITIONS
    //


    public boolean addTCEntry( List tcentry ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean addTCEntry( String namespace, String name,
        String version,
        String physicalname, TCType type,
        String resourceid,
        List pfnprofiles, List lfnprofiles,
        SysInfo system ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean addTCLfnProfile( String namespace, String name,
        String version,
        List profiles ) throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean addTCPfnProfile( String pfn, TCType type,
        String resourcename,
        List profiles ) throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

//deletions


    public boolean deleteTCbyLogicalName( String namespace, String name,
        String version, String resourceid,
        TCType type ) throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTCbyPhysicalName( String physicalname,
        String namespace,
        String name, String version,
        String resourceid, TCType type ) throws
        Exception {

        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTCbyType( TCType type, String resourceid ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTCbySysInfo( SysInfo sysinfo ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTCbyResourceId( String resourceid ) throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTC() throws Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    public boolean deleteTCPfnProfile( String physicalname, TCType type,
        String resourceid, List profiles ) {
        throw new UnsupportedOperationException( "Not Implemented" );

    }

    public boolean deleteTCLfnProfile( String namespace, String name,
        String version, List profiles ) {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    /**
     * Returns the contents of the entire TC
     * @return List of Transformation Catalog Objects
     * @throws Exception
     */
    public List getTC() throws Exception {
        List result=new ArrayList();
        for ( Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            for ( Iterator j = ( ( Map ) i.next() ).values().iterator();
                j.hasNext(); ) {
                for ( Iterator k = ( ( List ) j.next() ).iterator(); k.hasNext(); ) {
                    TransformationCatalogEntry tc = (
                        TransformationCatalogEntry ) k.next();
                    result.add(tc);
                }

            }
        }

    return result;
}

/**        List result = null;
        int[] length = {0, 0, 0, 0, 0};
        for ( Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            TransformationCatalogEntry tc = ( TransformationCatalogEntry ) i.
                next();
            if ( result == null ) {
                result = new ArrayList( 10 );
            }
            String[] s = {tc.getResourceId(),
                tc.getLogicalTransformation(),
                tc.getPhysicalTransformation(),
                tc.getType().toString(), tc.getSysInfo().toString(),
                ProfileParser.combine( tc.getProfiles() )};
            columnLength( s, length );
            result.add( s );
        }
        if ( result != null ) {
            result.add( length );
        }*/


    public boolean isClosed() {
    //not impelemented
    return true;
}

public void close() {
    //not impelemented
}



/**
 * populates the tree map
 * with the contents of the
 * transformation catalog.
 * The key for this is
 * poolname. With each pool is stored and lfn map whose key is the logicaltransformation and
 * and value is the corresponding
 * TransformationCatalogEntry object which corresponds
 * to the record in the TC.
 */

/**
 * Adds multiple entries into the TC.  Calls the above api multiple times.
 * @return boolean
 */
private boolean populateTC() {
    BufferedReader buf = null;
    // String profilestring = null;
    int linecount = 0;
    int count = 0;
    try {
        String line = null;
        buf = new BufferedReader( new FileReader( tcFile ) );
        while ( ( line = buf.readLine() ) != null ) {
            linecount++;
            if ( ! ( line.startsWith( "#" ) ||
                line.trim().equalsIgnoreCase( "" ) ) ) {
                TransformationCatalogEntry tc = new
                    TransformationCatalogEntry();
                String[] tokens = line.split( "[ \t]+", 4 );
                String profilestring = null;
                for ( int i = 0; i < tokens.length; i++ ) {
                    switch ( i ) {
                        case 0: //poolname
                            tc.setResourceId( tokens[ i ] );
                            break;
                        case 1: //logical transformation name
                            String logicaltransformation = null;
                            if ( tokens[ i ].indexOf( "__" ) != -1 ) {
                                mLogger.log(
                                    "Old style Logical transformation found. Converting to new style",
                                    LogManager.DEBUG_MESSAGE_LEVEL );
                                String[] ltr = convertOldFQLTRtoNew(
                                    tokens[ i ] );
                                logicaltransformation = Separator.combine(
                                    ltr[ 0 ],
                                    ltr[ 1 ], ltr[ 2 ] );
                            } else {
                                logicaltransformation = tokens[ i ];
                            }
                            tc.setLogicalTransformation(
                                logicaltransformation );
                            break;
                        case 2: //pfn
                            tc.setPhysicalTransformation( tokens[ i ] );
                            break;
                        case 3: //environment String
                            if ( !tokens[ i ].equalsIgnoreCase( "null" ) ) {
                                if ( tokens[ i ].equalsIgnoreCase(
                                    "INSTALLED" ) ) {
                                    mLogger.log(
                                        "This seems to be a new TC file format. Please set vds.tc.mode=File",
                                        LogManager.ERROR_MESSAGE_LEVEL );
                                } else {
                                    profilestring = tokens[ i ];
                                }
                            }
                            break;
                        default:
                            mLogger.log( "Line " + linecount +
                                " : Humm no need to be in default",
                                LogManager.ERROR_MESSAGE_LEVEL );
                    } //end of switch
                } //end of for loop
                //generate the profiles in the new format and store them
                if ( profilestring != null ) {
                    String[] p = profilestring.split( ";" );
                    for ( int i = 0; i < p.length; i++ ) {
                        String[] keyvalue = p[ i ].split( "=" );
                        if ( keyvalue.length == 2 &&
                            !keyvalue[ 1 ].equalsIgnoreCase( "" ) ) {
                            Profile pr = new Profile( "env",
                                keyvalue[ 0 ].trim(),
                                keyvalue[ 1 ].trim() );
                            tc.setProfile( pr );
                        }
                    }
                }
                tc.setType( TCType.INSTALLED );
                tc.setSysInfo( new SysInfo( null ) );

                Map lfnMap = null;
                if ( !mTreeMap.containsKey( tc.getResourceId() ) ) {
                    lfnMap = new TreeMap();
                    mTreeMap.put( tc.getResourceId(), lfnMap );
                } else {
                    lfnMap = ( Map ) mTreeMap.get( tc.getResourceId() );
                }
                List entries = null;
                if ( !lfnMap.containsKey( tc.getLogicalTransformation() ) ) {
                    entries = new ArrayList( 3 );
                    lfnMap.put( tc.getLogicalTransformation(), entries );
                } else {
                    entries = ( List ) lfnMap.get( tc.
                        getLogicalTransformation() );
                }
                entries.add( tc );
                count++;
            } //end of if "#"
        } //end of while line
        mLogger.log( "Loaded " + count + " entries to the TC Map",
            LogManager.DEBUG_MESSAGE_LEVEL );
        buf.close();
        return true;
    } catch ( FileNotFoundException ex ) {
        mLogger.log( "The tc text file " + tcFile + " was not found",
            LogManager.ERROR_MESSAGE_LEVEL );
        mLogger.log( "Considering it as Empty TC",
            LogManager.ERROR_MESSAGE_LEVEL );
        return true;
    } catch ( IOException e ) {
        mLogger.log( "Unable to open the file " + tcFile, e,
            LogManager.ERROR_MESSAGE_LEVEL );
        return false;
    } catch ( IllegalStateException e ) {
        mLogger.log( "On line " + linecount + "in File " + this.mTCFile, e,
            LogManager.ERROR_MESSAGE_LEVEL );
        return false;
    } catch ( Exception e ) {
        mLogger.log(
            "While loading entries into the map on line " + linecount +
            "\n", e, LogManager.ERROR_MESSAGE_LEVEL );
        return false;
    }
}

/*   private static String getKey( String poolName, String lfn, String type ) {
       String st = poolName + "_" + lfn + "_" + type;
       return st;
   }
 */
private static String[] convertOldFQLTRtoNew( String logicaltransformation ) {
    int start = logicaltransformation.indexOf( "__" );
    int end = logicaltransformation.lastIndexOf( "_" );
    String[] result = {logicaltransformation.substring( 0, start ),
        logicaltransformation.substring( start + 2, end ),
        logicaltransformation.substring( end + 1,
        logicaltransformation.length() )};
    return result;
}

/**
 * Computes the maximum column lenght for pretty printing.
 * @param s String[]
 * @param count int[]
 */
private static void columnLength( String[] s, int[] count ) {
    for ( int i = 0; i < count.length; i++ ) {
        if ( s[ i ].length() > count[ i ] ) {
            count[ i ] = s[ i ].length();
        }
    }

}

}
