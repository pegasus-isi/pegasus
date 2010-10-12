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


package org.griphyn.cPlanner.toolkit;


import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.common.util.FactoryException;

import edu.isi.pegasus.planner.visualize.Callback;
import edu.isi.pegasus.planner.visualize.KickstartParser;

import edu.isi.pegasus.planner.visualize.spaceusage.Plot;
import edu.isi.pegasus.planner.visualize.spaceusage.Ploticus;
import edu.isi.pegasus.planner.visualize.spaceusage.KickstartOutputFilenameFilter;
import edu.isi.pegasus.planner.visualize.spaceusage.SpaceUsage;
import edu.isi.pegasus.planner.visualize.spaceusage.SpaceUsageCallback;
import edu.isi.pegasus.planner.visualize.spaceusage.TailStatd;

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

public class PlotSpaceUsage extends Executable{

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
     * The size units.
     */
    private String mSizeUnits;

    /**
     * The time units.
     */
    private String mTimeUnits;

    /**
     * The timing source used to order the events.
     */
    private String mTimingSource;

    /**
     * A boolean indicating to use stat information for estimating
     * directory sizes.
     */
    private boolean mUseStatInfo;

    /**
     * Default constructor.
     */
    public PlotSpaceUsage(){
        super();
        mLogMsg = new String();
        mVersion = Version.instance().toString();
        mOutputDir = this.DEFAULT_OUTPUT_DIR;
        mLoggingLevel = 0;
        mSizeUnits    = "K";
        mTimeUnits    = null;
        mBasename     = "ploticus";
        mTimingSource = this.DEFAULT_TIMING_SOURCE;
        mUseStatInfo  = false;
    }

    /**
     * The main program.
     *
     *
     * @param args the main arguments passed to the plotter.
     */
    public static void main(String[] args) {

        PlotSpaceUsage me = new PlotSpaceUsage();
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

        //do sanity check on units
        mSizeUnits = mSizeUnits.trim();
        if ( mSizeUnits.length() != 1 ){
            throw new RuntimeException( "The valid size units can be K or M or G" );
        }

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

        //determing the callback on the basis of timing source
        Callback c;
        if( mTimingSource.equalsIgnoreCase( this.DEFAULT_TIMING_SOURCE )){
            c = new SpaceUsageCallback();
        }
        else if ( mTimingSource.equalsIgnoreCase( this.TAILSTATD_TIMING_SOURCE )){
            c = new TailStatd();
        }
        else{
            throw new RuntimeException( "No callback available for timing source" +
                                        mTimingSource );
        }
        mLogger.log( "Timing Source being used is " + mTimingSource ,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        c.initialize( mInputDir, mUseStatInfo );
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

        SpaceUsage s = (SpaceUsage)c.getConstructedObject();
        s.sort();
        log( " Space Store is \n" + c.getConstructedObject(),
             LogManager.DEBUG_MESSAGE_LEVEL);

        //generate the ploticus format
        Plot plotter = new Ploticus();
        plotter.initialize( mOutputDir, mBasename , mUseStatInfo);
        try{
            List result = plotter.plot( s, mSizeUnits.charAt( 0 ), mTimeUnits );

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

        Getopt g = new Getopt( "plot-space-usage", args,
                              "b:i:o:s:t:T:uhvV",
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

                case 's'://size-units
                    this.mSizeUnits = g.getOptarg();
                    break;

                case 't'://timing source
                    this.mTimingSource = g.getOptarg();
                    break;

                case 'T'://time units
                    this.mTimeUnits = g.getOptarg();
                    break;

                case 'u'://use-stat
                    this.mUseStatInfo = true;
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
        LongOpt[] longopts = new LongOpt[10];

        longopts[0]   = new LongOpt( "input", LongOpt.REQUIRED_ARGUMENT, null, 'i' );
        longopts[1]   = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[2]   = new LongOpt( "size-units", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[3]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[4]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[5]   = new LongOpt( "Version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[6]   = new LongOpt( "basename", LongOpt.REQUIRED_ARGUMENT, null, 'b' );
        longopts[7]   = new LongOpt( "timing-source", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[8]   = new LongOpt( "use-stat", LongOpt.NO_ARGUMENT, null, 'u' );
        longopts[9]   = new LongOpt( "time-units", LongOpt.REQUIRED_ARGUMENT, null, 'T' );
        return longopts;
    }


    /**
     * Prints out a short description of what the command does.
     */
    public void printShortVersion(){
        String text =
          "\n $Id$ " +
          "\n " + getGVDSVersion() +
          "\n Usage : plot-space-usage [-Dprop  [..]] -i <input directory>  " +
          " [-o output directory] [-b basename] [-s size units] [-t timing source] " +
          " [-T time units] [-u] [-v] [-V] [-h]";

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
           "\n plot-space-usage - A plotting tool that plots out the space usage on remote clusters over time"  +
           "\n Usage: plot_space_usage [-Dprop  [..]] --dir <input directory> [--base basename] " +
           "\n [--output output directory] [--timing-source source] [--use-stat] [--verbose] [--Version] [--help] " +
           "\n" +
           "\n Mandatory Options " +
           "\n --input              the directory where the kickstart records reside." +
           "\n Other Options  " +
           "\n -b |--basename      the basename prefix for constructing the ploticus files." +
           "\n -o |--output        the output directory where to generate the ploticus files." +
           "\n -s |--size-units    the units in which you want the filesizes to be plotted (can be K or M or G)."  +
           "\n -t |--timing-source the source from which the ordering of events is determined. " +
           "\n                     Can be kickstart or tailstatd. Defaults to kickstart." +
           "\n -T |--time-units    the units in which you want the x axis to be plotted (seconds|minutes|hours) Defaults to seconds." +
           "\n -u |--use-stat      use the file stat information in kickstart records to estimate directory usage" +
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




