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

package org.griphyn.cPlanner.toolkit;


import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.FactoryException;

import org.griphyn.cPlanner.visualize.Callback;
import org.griphyn.cPlanner.visualize.KickstartParser;


import org.griphyn.cPlanner.visualize.spaceusage.KickstartOutputFilenameFilter;

import org.griphyn.cPlanner.visualize.WorkflowMeasurements;

import org.griphyn.cPlanner.visualize.nodeusage.NodeUsageCallback;
import org.griphyn.cPlanner.visualize.nodeusage.Ploticus;

import org.griphyn.vdl.toolkit.FriendlyNudge;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * This parses the kickstart records and generates input files for ploticus,
 * to visualize.
 *
 *
 * @author Karan Vahi
 *
 * @version $Revision$
 */

public class PlotNodeUsage extends Executable{

    /**
     * The default output directory.
     */
    public static final String DEFAULT_OUTPUT_DIR = ".";

    /**
     * The default timing source.
     */
    public static final String DEFAULT_TIMING_SOURCE = "Kickstart";

    /**
     * The tailstatd timing source.
     */
    public static final String TAILSTATD_TIMING_SOURCE = "Tailstatd";


    /**
     * The input directory containing the kickstart records.
     */
    private String mInputDir;

    /**
     * The output directory where to generate the ploticus output.
     */
    private String mOutputDir;

    /**
     * The default basename given to the files.
     */
    private String mBasename;

    /**
     * The logging level to be used.
     */
    private int mLoggingLevel;

    /**
     * The time units.
     */
    private String mTimeUnits;

    /**
     * Default constructor.
     */
    public PlotNodeUsage(){
        super();
        mLogMsg = new String();
        mVersion = Version.instance().toString();
        mOutputDir = this.DEFAULT_OUTPUT_DIR;
        mLoggingLevel = 0;
        mBasename     = "ploticus";
    }

    /**
     * The main program.
     *
     *
     * @param args the main arguments passed to the plotter.
     */
    public static void main(String[] args) {

        PlotNodeUsage me = new PlotNodeUsage();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;

        try{
            me.executeCommand( args );
        }
        catch ( FactoryException fe){
            me.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        }
        catch ( RuntimeException rte ) {
            //catch all runtime exceptions including our own that
            //are thrown that may have chained causes
            me.log( convertException(rte),
                         LogManager.FATAL_MESSAGE_LEVEL );
            result = 1;
        }
        catch ( Exception e ) {
            //unaccounted for exceptions
            me.log(e.getMessage(),
                         LogManager.FATAL_MESSAGE_LEVEL );
            e.printStackTrace();
            result = 3;
        } finally {
            double endtime = new Date().getTime();
            execTime = (endtime - starttime)/1000;
        }

        // warn about non zero exit code
        if ( result != 0 ) {
            me.log("Non-zero exit-code " + result,
                         LogManager.WARNING_MESSAGE_LEVEL );
        }
        else{
            //log the time taken to execute
            me.log("Time taken to execute is " + execTime + " seconds",
                         LogManager.INFO_MESSAGE_LEVEL);
        }

        System.exit(result);
    }




    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     */
    public void executeCommand(String[] args) {
        parseCommandLineArguments(args);

        //set logging level only if explicitly set by user
        if( mLoggingLevel > 0 ) { mLogger.setLevel( mLoggingLevel ); }


        //do sanity check on input directory
        if( mInputDir == null ){
            throw new RuntimeException(
                "You need to specify the directory containing kickstart records");

        }
        File dir = new File( mInputDir );
        if ( dir.isDirectory() ){
            //see if it is readable
            if ( !dir.canRead() ){
                throw new RuntimeException( "Cannot read directory " + mInputDir);
            }
        }
        else{
            throw new RuntimeException( mInputDir + " is not a directory " );
        }

        //sanity check on output directory
        dir = new File( mOutputDir );
        if( dir.exists() ){
            //directory already exists.
            if ( dir.isDirectory() ){
                if ( !dir.canWrite() ){
                    throw new RuntimeException( "Cannot write out to output directory " +
                                                mOutputDir );
                }
            }
            else{
                //directory is a file
                throw new RuntimeException( mOutputDir + " is not a directory ");
            }

        }
        else{
            dir.mkdirs();
        }

        KickstartParser su = new KickstartParser();

        Callback c = new NodeUsageCallback();

        c.initialize( mInputDir, true );
        su.setCallback( c );

        //String dir = "/usr/sukhna/work/test/dags/ivdgl1/blackdiamond/run0004";
        File directory = new File( mInputDir );
        String[] files = directory.list( new KickstartOutputFilenameFilter() );
        for( int i = 0; i < files.length; i++){
            String file = mInputDir + File.separator +  files[i];

            try {
                log( "Parsing file " + file , LogManager.DEBUG_MESSAGE_LEVEL );
                su.parseKickstartFile(file);
            }
            catch (IOException ioe) {
                log( "Unable to parse kickstart file " + file + convertException( ioe ),
                     LogManager.DEBUG_MESSAGE_LEVEL);
            }
            catch( FriendlyNudge fn ){
                log( "Problem parsing file " + file + convertException( fn ),
                     LogManager.WARNING_MESSAGE_LEVEL );
            }
        }

        //we are done with parsing
        c.done();

        WorkflowMeasurements wm = ( WorkflowMeasurements )c.getConstructedObject();
        wm.sort();
        log( " Workflow Measurements is \n" + wm,
             LogManager.DEBUG_MESSAGE_LEVEL);

        //generate the ploticus format
        Ploticus plotter = new Ploticus();
        plotter.initialize( mOutputDir, mBasename , true);
        try{
            List result = plotter.plot( wm, '0', mTimeUnits );

            for( Iterator it = result.iterator(); it.hasNext(); ){
                mLogger.log( "Written out file " + it.next(),
                             LogManager.INFO_MESSAGE_LEVEL );
            }
        }
        catch (IOException ioe) {
            log( "Unable to plot the files " + convertException( ioe ),
                 LogManager.DEBUG_MESSAGE_LEVEL);
        }

    }


    /**
     * Parses the command line arguments using GetOpt and returns a
     * <code>PlannerOptions</code> contains all the options passed by the
     * user at the command line.
     *
     * @param args  the arguments passed by the user at command line.
     */
    public void parseCommandLineArguments(String[] args){
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt( "plot-node-usage", args,
                              "b:i:o:T:hvV",
                              longOptions, false);
        g.setOpterr(false);

        int option = 0;

        while( (option = g.getopt()) != -1){
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 'b'://the basename
                    this.mBasename = g.getOptarg();
                    break;

                case 'i'://dir
                    this.mInputDir =  g.getOptarg();
                    break;

                case 'h'://help
                    printLongVersion();
                    System.exit( 0 );
                    return;

                case 'o'://output directory
                    this.mOutputDir =  g.getOptarg();
                    break;

                case 'T'://time units
                    this.mTimeUnits = g.getOptarg();
                    break;

                case 'v'://verbose
                    mLoggingLevel++;
                    break;

                case 'V'://version
                    mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
                    System.exit(0);


                default: //same as help
                    printShortVersion();
                    throw new RuntimeException("Incorrect option or option usage " +
                                               (char)option);

            }
        }

    }



    /**
     * Tt generates the LongOpt which contain the valid options that the command
     * will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid
     * options
     */
    public LongOpt[] generateValidOptions(){
        LongOpt[] longopts = new LongOpt[7];

        longopts[0]   = new LongOpt( "input", LongOpt.REQUIRED_ARGUMENT, null, 'i' );
        longopts[1]   = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[2]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[3]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[4]   = new LongOpt( "Version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[5]   = new LongOpt( "basename", LongOpt.REQUIRED_ARGUMENT, null, 'b' );
        longopts[6]   = new LongOpt( "time-units", LongOpt.REQUIRED_ARGUMENT, null, 'T' );
        return longopts;
    }


    /**
     * Prints out a short description of what the command does.
     */
    public void printShortVersion(){
        String text =
          "\n $Id$ " +
          "\n " + getGVDSVersion() +
          "\n Usage : plot_node_usage [-Dprop  [..]] -i <input directory>  " +
          " [-o output directory] [-b basename] [-T time units] [-v] [-V] [-h]";

        System.out.println(text);
    }

    /**
     * Prints the long description, displaying in detail what the various options
     * to the command stand for.
     */
    public void printLongVersion(){

        String text =
           "\n $Id$ " +
           "\n " + getGVDSVersion() +
           "\n plot-node-usage - A plotting tool that plots out the number of jobs running on remote clusters over time"  +
           "\n Usage: plot_node_usage [-Dprop  [..]] --input <input directory> [--base basename] " +
           "\n [--output output directory] [-T time units] [--verbose] [--Version] [--help] " +
           "\n" +
           "\n Mandatory Options " +
           "\n --input              the directory where the kickstart records reside." +
           "\n Other Options  " +
           "\n -b |--basename      the basename prefix for constructing the ploticus files." +
           "\n -o |--output        the output directory where to generate the ploticus files." +
           "\n -T |--time-units    the units in which you want the x axis to be plotted (seconds|minutes|hours) " +
           "\n                     Defaults to seconds." +
           "\n -v |--verbose       increases the verbosity of messages about what is going on" +
           "\n -V |--version       displays the version of the Pegasus Workflow Planner" +
           "\n -h |--help          generates this help." +
           "\n The following exitcodes are produced" +
           "\n 0 plotter was able to generate plots" +
           "\n 1 an error occured. In most cases, the error message logged should give a" +
           "\n   clear indication as to where  things went wrong." +
           "\n 2 an error occured while loading a specific module implementation at runtime" +
           "\n ";

        System.out.println(text);
        //mLogger.log(text,LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Loads all the properties that would be needed by the Toolkit classes.
     */
    public void loadProperties(){
        //empty for time being
    }

}




