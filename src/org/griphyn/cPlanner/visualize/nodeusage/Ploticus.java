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


package org.griphyn.cPlanner.visualize.nodeusage;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.visualize.WorkflowMeasurements;
import org.griphyn.cPlanner.visualize.Measurement;

import java.util.List;
import java.util.Iterator;

import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import java.text.NumberFormat;
import java.text.DecimalFormat;

/**
 * An implementation that plots in the Ploticus format.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Ploticus {

    /**
     * The default timing units.
     */
    public static final String DEFAULT_TIMING_UNITS = "seconds";

    /**
     * The minutes unit for the x axis.
     */
    public static final String MINUTES_TIMING_UNITS = "minutes";

    /**
     * The minutes unit for the x axis.
     */
    public static final String HOURS_TIMING_UNITS = "hours";


    /**
     * The directory where the files are to be generated.
     *
     */
    private String mDirectory;

    /**
     * The basename of the files.
     */
    private String mBasename;

    /**
     * A boolean indicating whether to use stat info or not.
     */
    private boolean mUseStatInfo;

    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The number formatter to format the float entries.
     */
    private NumberFormat mNumFormatter;

    /**
     * The time units.
     */
    private String mTimeUnits;


    /**
     * The default constructor.
     *
     */
    public Ploticus(){
        mLogger    =  LogManagerFactory.loadSingletonInstance();
        mDirectory = ".";
        mBasename  = "ploticus" ;
        mNumFormatter = new DecimalFormat( "0.000" );
        mTimeUnits = this.DEFAULT_TIMING_UNITS;
    }

    /**
     * Initializer method.
     *
     * @param directory  the directory where the plots need to be generated.
     * @param basename   the basename for the files that are generated.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory , String basename, boolean useStatInfo ){
        mDirectory = directory;
        mBasename  = basename;
        mUseStatInfo = useStatInfo;
    }

    /**
     * Plot out the space usage. Writes out a Ploticus data file.
     *
     * @param wm       the workflow measurements.
     * @param unit     the unit in which we need to plot the number.
     * @param timeUnits units in which to plot time.
     *
     * @return List of file pathnames for the files that are written out.
     *
     * @exception IOException in case of unable to write to the file.
     */
    public List plot( WorkflowMeasurements wm, char unit , String timeUnits ) throws IOException{
        //first let us sort on the timestamps
        wm.sort();

        String site;

        List result = new ArrayList( 2 );

        //sanity check on time units
        mTimeUnits = ( timeUnits == null )?  this.DEFAULT_TIMING_UNITS : timeUnits;


        //go thru space usage for each site.
        for( Iterator it = wm.siteIterator(); it.hasNext(); ){
            site = ( String ) it.next();

            String dataFile   = getFilename( site, "_nu.dat" );
            String scriptFile = getFilename( site, "_nu.pl" );
            result.add( dataFile );
            result.add( scriptFile );

            PrintWriter dataPW = new PrintWriter( new FileWriter( dataFile ) );
            mLogger.log(  "Will write out to " + dataFile  + "," + scriptFile,
                          LogManager.DEBUG_MESSAGE_LEVEL );

            long min = 0; boolean first = true;
            long absTime, time = 0; //in seconds
            long currJobs = 0;
            long maxJobs  = 0;
            float cTime = 0;

            //go through space usage for a particular site
            for ( Iterator sizeIT = wm.getMeasurements( site ).iterator(); sizeIT.hasNext(); ){
                Measurement m   = ( Measurement ) sizeIT.next();
                absTime   = m.getTime().getTime();
                currJobs  = ((Integer)m.getValue()).intValue();

                if ( first ) {
                    min = absTime;
                    first = false;
                }


                //calculate the relative time in seconds
                time = ( absTime - min ) / 1000;

                //convert time from seconds to units specified
                cTime = convertFromSecondsTo( time, timeUnits );

                //update the max space
                if ( currJobs > maxJobs ){
                    maxJobs = currJobs;
                }



                //log the entry in the data file.
                String entry  = constructEntry( m.getJobName(), cTime, currJobs);
                mLogger.log( entry, LogManager.DEBUG_MESSAGE_LEVEL );
                dataPW.println( entry );


            }

            //the value in time right now it the max time
            generateScriptFile( scriptFile, dataFile,
                                new Character(unit).toString(),
                                cTime, maxJobs );

            //close and flush to file per site
            dataPW.close();
        }



        return result;
    }


    /**
     * Generates the script file required to give as input to ploticus.
     *
     * @param name     the path to the script file.
     * @param dataFile the path to corresponding data file.
     * @param yUnits   the units for the space value.
     * @param maxX     the time in seconds.
     * @param maxY     the maximum space.
     */
    public void generateScriptFile( String name, String dataFile, String yUnits,
                                    float maxX, float maxY )
           throws IOException{

        PrintWriter writer = new PrintWriter( new FileWriter( name ) );

        //write the page proc
        writer.println( "#proc page" );
        writer.println( "#if @DEVICE in png,gif" );
        writer.println( "\t scale: 0.6" );
        writer.println( "#endif" );
        writer.println();

        //write the getdata proc
        writer.println( "#proc getdata" );
        writer.print( "file: ");
        writer.println(  new File(dataFile).getName() );
        writer.println( " fieldnames: time number_of_jobs" );
        writer.println();

        //write out area defn
        writer.println( "#proc areadef" );
        writer.println( "title: Number of jobs running over time" );
        writer.println( "titledetails: size=14  align=C" );
        writer.println( "rectangle: 1 1 8 4" );

        /* we let ploticus worry about ranges */
//        writer.print( "xrange: 0 " );
//        //round to the latest 100
//        long modTime  = ( maxTime/100 + 1 )* 100 ;
//        //round space to latest 100 if > 0
//        float modSpace = maxSpace > 1 ?
//                         (new Float(maxSpace/100).intValue() + 1)* 100:
//                         maxSpace;
//        writer.println( modTime );
//        writer.print( "yrange: 0 " );
//        writer.println( modSpace );
//        writer.println();

        writer.println( "xautorange datafield=1" );
        writer.println( "yautorange datafield=2 lowfix=0" );//y axis always starts from 0
        //round to the latest 100
        float modTime  = ( maxX/100 + 1 )* 100 ;
        //round space to latest 100 if > 0
        float modSpace = maxY > 1 ?
                         (new Float(maxY/10).intValue() + 1)* 10:
                         maxY;


        //we want 15-16 points on the x axis
        float xIncrement = ( (modTime/150)  + 1 ) * 10;
        writer.println( "#proc xaxis" );
        writer.print( "stubs: inc " );
        writer.println( xIncrement );
        writer.print( "minorticinc: " );
        writer.println( xIncrement/2 );
        writer.print(  "label: time in " );
        writer.println( mTimeUnits );
        writer.println();

        //we want 10 points on the y axis
        float yIncrement =   modSpace/10;
        writer.println( "#proc yaxis" );
        writer.print( "stubs: inc " );
        writer.println( yIncrement );
        writer.print( "minorticinc: " );
        writer.println( yIncrement/2 );
        writer.println( "gridskip: min" );
        //writer.println( "ticincrement: 100 1000" );
        writer.println( "label: number of jobs running " );
        writer.println();

        writer.println( "#proc lineplot" );
        writer.println( "xfield: time" );
        writer.println( "yfield: number_of_jobs" );
        writer.println( "linedetails: color=blue width=.5" );
        writer.println();

        writer.println( "#proc legend" );
        writer.println( "location: max-1 max" );
        writer.println( "seglen: 0.2" );

        writer.close();
    }


    /**
     * Returns the filename of the ploticus file to be generated.
     *
     * @param site   the site handle.
     * @param suffix the suffix to be applied to the file.
     *
     * @return the path to the file.
     */
    protected String getFilename( String site, String suffix ){
        StringBuffer sb = new StringBuffer();
        sb.append( mDirectory ).append( File.separator ).append( mBasename ).
           append( "-" ).append( site ).append( suffix );

        return sb.toString();
    }

    /**
     * Returns an entry that needs to be plotted in the graph.
     *
     * @param  jobname        the name of the associated job.
     * @param  time           the time
     * @param  measurement    measurement
     *
     * @return the entry to be logged
     */
    protected String constructEntry( String job, float time, long measurement ){

        StringBuffer sb = new StringBuffer();
        sb.append( mNumFormatter.format( time ) ).append( "\t" )
          .append( measurement ).append( "\t" )
          .append( job );

        return sb.toString();
    }

    /**
     * Converts from seconds to one of the units specified.
     *
     * @param time    the time.
     * @param units   the units
     *
     * @return converted value in long.
     */
    private float convertFromSecondsTo( long time, String units ){
        if( !validTimeUnits( units) ){
            throw new RuntimeException( "Unsupported time units " + units);
        }

        if( units == this.DEFAULT_TIMING_UNITS ){
            return time;
        }

        float result;

        float factor = ( units.equals( this.MINUTES_TIMING_UNITS ) ) ?
                                                             60 :
                                                            (units.equals( HOURS_TIMING_UNITS ) )?
                                                                                                3600:
                                                                                                -1;

        result = ( time/(int)factor + (time % factor)/factor );

        return result;
    }

    /**
     * Returns a boolean indicating if a valid time unit or not.
     *
     * @param unit  the time unit.
     *
     * @return boolean
     */
    private boolean validTimeUnits( String units ){

        return ( units.equals( this.DEFAULT_TIMING_UNITS) ||
                 units.equals( this.MINUTES_TIMING_UNITS ) ||
                 units.equals( this.HOURS_TIMING_UNITS )    ) ;

    }


}
