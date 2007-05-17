/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.visualize;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.vdl.util.ChimeraProperties;

import org.griphyn.vdl.directive.ParseKickstart;

import org.griphyn.vdl.invocation.InvocationRecord;
import org.griphyn.vdl.invocation.StatCall;
import org.griphyn.vdl.invocation.Data;
import org.griphyn.vdl.invocation.Regular;

import org.griphyn.vdl.parser.InvocationParser;


import org.griphyn.common.util.Currently;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * A helper class that parses the kickstart records and has calls to callbacks
 * for working on data sections of standard out, standard error, and standard
 * input.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.3 $
 */

public class KickstartParser {

    /**
     * The parser class that is used to parse a kickstart record and return
     * the invocation record.
     */
    private ParseKickstart mParseKickstart;

    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The callback object.
     */
    private Callback mCallback;

    /**
     * Semi-singleton, dynamically instantiated once for the lifetime.
     * The properties determine which Xerces parser is being used.
     */
    private InvocationParser mInvocationParser;


    /**
     * The default constructor.
     */
    public KickstartParser(){
        mLogger = LogManager.getInstance();
    }


    /**
     * Sets the callback to which to callout to while parsing a kickstart
     * record.
     *
     * @param c  the Callback to call out to.
     */
    public void setCallback( Callback c ){
        mCallback = c;
        mLogger = LogManager.getInstance();
    }

    /**
     *
     */
    public List parseKickstartFile( String file ) throws IOException{
        List result = new ArrayList();


        //sanity check
        if ( mCallback == null ){ throw new RuntimeException( "Callback not initialized" ); }

        //initialize the parser if required
        if ( mParseKickstart == null ){ mParseKickstart = new ParseKickstart( ); }

        // get access to the invocation parser
        if ( mInvocationParser == null ) {
            ChimeraProperties props = ChimeraProperties.instance();
            String psl = props.getPTCSchemaLocation();
            mLogger.log( "Using XML schema location " + psl,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mInvocationParser = new InvocationParser( psl );
        }

        //do some sanity checks for the file.


        //extract to memory
        File f = new java.io.File( file );
        List extract = mParseKickstart.extractToMemory( f );


        org.griphyn.vdl.invocation.File invocationFile = null;

        // testme: for each record obtained, work on it
        for ( int j=1; j-1 < extract.size(); ++j ) {
            String temp = (String) extract.get( j-1 );

            // test 5: try to parse XML
            InvocationRecord invocation = mInvocationParser.parse( new StringReader(temp) );
            mCallback.cbInvocationStart( getJobName( f.getName() ),  invocation.getResource() );

            //get the data about the various jobs
            List jobs = invocation.getJobList();

            //callback for the data sections of various streams
            List stats = invocation.getStatList();
            StringWriter writer = new StringWriter();
            for ( Iterator it = stats.iterator(); it.hasNext(); ){
                StatCall statC = ( StatCall )it.next();
                String handle = statC.getHandle();
                invocationFile = statC.getFile();

                //call out appropriate callback functions with the data
                char c = handle.charAt( 0 );
                Data data = statC.getData();
                String value =  ( data == null ) ? "" : data.getValue();
                switch ( c ){
                    case 's': //stdout, //stderr,//stdin
                        if ( handle.equals( "stdout" ) ){
                            mCallback.cbStdOut( jobs, value );
                        }
                        else if( handle.equals( "stdin" ) ){
                            mCallback.cbStdIN( jobs, value );
                        }
                        else if( handle.equals( "stderr" ) ){
                            mCallback.cbStdERR( jobs, value );
                        }
                        break;

                    case 'i'://initial
                        if ( handle.equals( "initial" ) ){
                            if( invocationFile instanceof Regular ){
                                //we are interested in Regular files only
                                mCallback.cbInputFile( ((Regular)invocationFile).getFilename() , statC.getStatInfo() );
                            }
                        }
                        break;

                    case 'f'://final
                        if ( handle.equals( "final" ) ){
                            if( invocationFile instanceof Regular ){
                                //we are interested in Regular files only
                                mCallback.cbOutputFile( ((Regular)invocationFile).getFilename() , statC.getStatInfo() );
                            }

                        }

                    default:
                        break;
                }
            }

            //successfully done with an invocation record
            mCallback.cbInvocationEnd();
        }
        return result;
    }

    /**
     * Returns the name of the job from the kickstart output filename.
     *
     * @param outName  the name of the out file.
     *
     * @return the job name.
     */
    protected String getJobName( String outName ){
        return outName.substring( 0, outName.indexOf( '.' ));
    }

}
