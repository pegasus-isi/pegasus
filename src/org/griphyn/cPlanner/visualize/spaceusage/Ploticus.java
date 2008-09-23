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


package org.griphyn.cPlanner.visualize.spaceusage;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

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

public class Ploticus implements Plot {

    /**
     * The size of an empty directory as reported by du -s
     */
    public static final String EMPTY_DIRECTORY_SIZE = "4K";

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
        mTimeUnits  = this.DEFAULT_TIMING_UNITS;
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
     * @param su    the SpaceUsage.
     * @param unit  the unit in which we need to plot. (K,M,G)
     * @param timeUnits   the time unit.
     *
     * @return List of file pathnames for the files that are written out.
     *
     * @exception IOException in case of unable to write to the file.
     */
    public List plot( SpaceUsage su, char unit, String timeUnits ) throws IOException{
        //first let us sort on the timestamps
        su.sort();

        String site;

        List result = new ArrayList( 2 );

        //sanity check on time units
        mTimeUnits = ( timeUnits == null )?  this.DEFAULT_TIMING_UNITS : timeUnits;

        //get the size of the empty directory in appropriate units
        float empty = new Space( new java.util.Date(),
                                 this.EMPTY_DIRECTORY_SIZE).getSize( unit ) ;

        //go thru space usage for each site.
        for( Iterator it = su.siteIterator(); it.hasNext(); ){
            site = ( String ) it.next();

            String dataFile   = getFilename( site, ".dat" );
            String scriptFile = getFilename( site, ".pl" );
            result.add( dataFile );
            result.add( scriptFile );

            PrintWriter dataPW = new PrintWriter( new FileWriter( dataFile ) );
            mLogger.log(  "Will write out to " + dataFile  + "," + scriptFile,
                          LogManager.DEBUG_MESSAGE_LEVEL );

            float cummulative_cln_size = 0;//tracks the space that has been cleaned up
            float curr_size       = 0; //stores the current size
            float clnup = 0;

            boolean first = true;
            long minTime = 0,absTime, time = 0; //in seconds

            float maxSpace = 0;
            float cTime = 0;

            //go through space usage for a particular site
            for ( Iterator sizeIT = su.getSizes( site ).iterator(); sizeIT.hasNext(); ){
                Space s   = (Space) sizeIT.next();
                absTime   = s.getDate().getTime();
                curr_size = s.getSize( unit );


                if ( first ) {
                    minTime = absTime;
                    first = false;
                }


                //if the difference is >0 means data was cleaned up
                //add to cummulative size
                //cummulative_cln_size += ( diff > 0 ) ? diff : 0;

                //calculate the relative time in seconds
                time = ( absTime - minTime ) / 1000;

                //convert time from seconds to units specified
                cTime = convertFromSecondsTo( time, timeUnits );

                //if data is regarding amount cleaned up add to cummulative size
                if( s.getCleanupFlag() ){
                    //subtract 4K overhead of directory size
                    //only if not use statinfo
                    clnup =  mUseStatInfo ? curr_size : curr_size - empty;
                    cummulative_cln_size += clnup;

                    mLogger.log( cTime + " job " + s.getAssociatedJob() + " cleans up " + clnup
                                 + unit ,
                                LogManager.DEBUG_MESSAGE_LEVEL );
                    mLogger.log( " Cummulative cleaned up size is now " + cummulative_cln_size,
                                 LogManager.DEBUG_MESSAGE_LEVEL );

                    //do not log just proceed
                    continue;
                }

                //update the max space
                if ( cummulative_cln_size + curr_size > maxSpace ){
                    maxSpace =  cummulative_cln_size + curr_size;
                }



                //log the entry in the data file.
                String entry  = constructEntry( s.getAssociatedJob(), cTime,
                                                curr_size,
                                                ( cummulative_cln_size + curr_size));
                mLogger.log( entry, LogManager.DEBUG_MESSAGE_LEVEL );
                dataPW.println( entry );


            }

            //the value in time right now it the max time
            generateScriptFile( scriptFile, dataFile,
                                new Character(unit).toString(),
                                cTime, maxSpace );

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
     * @param maxTime  the time in seconds.
     * @param maxSpace the maximum space.
     */
    public void generateScriptFile( String name, String dataFile, String yUnits,
                                    float maxTime, float maxSpace )
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
        writer.println( " fieldnames: time with_cleanup without_cleanup jobname" );
        writer.println();

        //write out area defn
        writer.println( "#proc areadef" );
        writer.println( "title: Remote Storage used over time" );
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
        writer.println( "yautorange datafield=3 lowfix=0" );//y axis always starts from 0
        writer.println();

        //round to the latest 100
        float modTime  = ( maxTime/100 + 1 )* 100 ;
        //round space to latest 100 if > 0
        float modSpace = maxSpace > 1 ?
                         (new Float(maxSpace/100).intValue() + 1)* 100:
                         maxSpace;


        //we want 15-16 points on the x axis
        float xIncrement = ( (modTime/150)  + 1 ) * 10;
        writer.println( "#proc xaxis" );
        writer.print( "stubs: inc " );
        writer.println( (int)xIncrement );
        writer.print( "minorticinc: " );
        writer.println( (int)(xIncrement/2) );
        writer.print(  "label: time in " );
        writer.println( mTimeUnits );
        writer.println();

        //we want 10 points on the y axis
        float yIncrement = modSpace > 1?
                           ( (modSpace/100)  + 1 ) * 10:
                           modSpace/10;
        writer.println( "#proc yaxis" );
        writer.print( "stubs: inc " );
        writer.println( yIncrement );
        writer.print( "minorticinc: " );
        writer.println( yIncrement/2 );
        writer.println( "gridskip: min" );
        //writer.println( "ticincrement: 100 1000" );
        writer.println( "label: space used in " + yUnits );
        writer.println( "labeldistance: 0.6" );
        writer.println();

        writer.println( "#proc lineplot" );
        writer.println( "xfield: time" );
        writer.println( "yfield: with_cleanup" );
        writer.println( "linedetails: color=blue width=.5" );
        writer.println( "legendlabel: with cleanup " );
        writer.println();

        //generate the cleanup jobs using a scatter plot
        writer.println( "#proc scatterplot" );
        writer.println( "xfield: time" );
        writer.println( "yfield: with_cleanup" );
        writer.println( "symbol: shape=circle fillcolor=red radius=0.04" );
        writer.println( "select:  @@jobname like cln_*" );
        writer.println( "legendlabel: cleanup nodes" );
        writer.println( );
//        using scatter plot now
//        //we want only the cleanup jobs to appear
//        writer.println( "pointsymbol: shape=circle fillcolor=blue radius=0.0" );
//        writer.println( "altsymbol: shape=circle fillcolor=red radius=0.04" );
//        writer.println( "altwhen:  @@jobname like cln_*" );//only plot points for cleanup jobs
//        writer.println();


        writer.println( "#proc lineplot" );
        writer.println( "xfield: time" );
        writer.println( "yfield: without_cleanup" );
        writer.println( "linedetails: style=1 dashscale=3 color=green width=.5" );
        writer.println( "legendlabel: without cleanup " );

        writer.println();

        writer.println( "#proc legend" );
        writer.println( "location: min+1 max+0.5" );
        writer.println( "format: singleline" );

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
     * @param  clnup_size     the size with cleanup
     * @param  no_clnup_size  the size without cleanup
     *
     * @return the entry to be logged
     */
    protected String constructEntry( String job, float time, float clnup_size,
                                     float no_clnup_size ){

        StringBuffer sb = new StringBuffer();
        sb.append( mNumFormatter.format( time ) ).append( "\t" )
          .append( mNumFormatter.format( clnup_size ) ).append( "\t" )
          .append( mNumFormatter.format( no_clnup_size ) ).append( "\t" )
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
