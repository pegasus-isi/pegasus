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

package edu.isi.pegasus.planner.client;


import edu.isi.pegasus.planner.ranking.GetDAX;
import edu.isi.pegasus.planner.ranking.Rank;
import edu.isi.pegasus.planner.ranking.Ranking;

import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.toolkit.Executable;

import org.griphyn.cPlanner.poolinfo.SiteFactory;
import org.griphyn.cPlanner.poolinfo.SiteFactoryException;
import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;


import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.transformation.TransformationFactory;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

import java.util.StringTokenizer;
import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.Date;

import org.griphyn.cPlanner.toolkit.CPlanner;

import org.griphyn.common.util.FactoryException;
import org.griphyn.common.catalog.transformation.Mapper;


/**
 * A client that ranks the DAX'es corresponding to the request id.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RankDAX extends Executable {

    /**
     * The base directory where the ranked daxes are kept.
     */
    private String mBaseDir;

    /**
     * The list of grid sites where the daxes can run.
     */
    private List mSites;

    /**
     * The output file that lists the daxes in sorted order.
     */
    private String mOutputFile;

    /**
     * The request id to get the daxes.
     */
    private String mRequestID;

    /**
     * The bag of objects that Pegasus requires.
     */
    private PegasusBag mBag;

    /**
     * The options to be passed ahead to pegasus plan.
     */
    private PlannerOptions mPlannerOptions;

    /**
     * The top n workflows to execute and put in the rankings file
     */
    private int mTopNum;

    /**
     * The default constructor.
     */
    public RankDAX() {
        super();
        mBag = new PegasusBag();
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        mTopNum = Integer.MAX_VALUE;
    }

    /**
     * The main program for the CPlanner.
     *
     *
     * @param args the main arguments passed to the planner.
     */
    public static void main(String[] args) {

        RankDAX me = new RankDAX();
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
            rte.printStackTrace();
            result = 1;
        }
        catch ( Exception e ) {
            //unaccounted for exceptions
            me.log(e.getMessage(),
                         LogManager.FATAL_MESSAGE_LEVEL );
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

        System.exit( result );
    }


    /**
     * Parses the command line arguments using GetOpt and sets the class
     * member variables.
     *
     * @param args  the arguments passed by the user at command line.
     *
     *
     */
    public void parseCommandLineArguments(String[] args){
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt("rank-dax", args, "vhr:d:s:o:r:f:t:", longOptions, false);
        g.setOpterr(false);

        int option = 0;
        int level = 0;
        while ( (option = g.getopt()) != -1) {
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 'd': //base directory
                    mBaseDir = g.getOptarg();
                    break;

                case 's': //comma separated list of sites
                    mSites = this.generateList( g.getOptarg() );
                    break;

                case 'o': //the output file where the ranked list is kept
                    mOutputFile = g.getOptarg();
                    break;

                case 'r': //the request id
                    mRequestID = g.getOptarg();
                    break;

                case 'v': //sets the verbosity level
                    level++;
                    break;

                case 'f'://the options to be passed to pegasus-plan
                    mPlannerOptions = new CPlanner().parseCommandLineArguments( g.getOptarg().split( "\\s" ) );
                    mBag.add( PegasusBag.PLANNER_OPTIONS , mPlannerOptions );
                    break;

                case 't'://rank top t
                    mTopNum = new Integer( g.getOptarg() ).intValue();
                    break;

                case 'h':
                    printShortHelp();
                    System.exit( 0 );
                    break;

                default: //same as help
                    printShortHelp();
                    throw new RuntimeException("Incorrect option or option usage " +
                                               option);

            }
        }
        if( level > 0 ){
            mLogger.setLevel( level );
        }
    }

    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     */
    public void executeCommand( String[] args ) {
        parseCommandLineArguments(args);


        if( mRequestID == null  || mPlannerOptions == null ){
            mLogger.log( "\nNeed to specify the request id and options that are to be passed to planner.",
                         LogManager.INFO_MESSAGE_LEVEL );

            this.printShortVersion();
            return;
        }

        //override the sites if any are set in the forward options
        mPlannerOptions.setExecutionSites( mSites );

        //load the site catalog using the factory
        PoolInfoProvider sCatalog = SiteFactory.loadInstance( mProps, false );
        mBag.add( PegasusBag.SITE_CATALOG, sCatalog );

        //load the transformation catalog using the factory
        TransformationCatalog tCatalog = TransformationFactory.loadInstance( mProps );
        mBag.add( PegasusBag.TRANSFORMATION_CATALOG, tCatalog );

        //initialize the transformation mapper
        mBag.add( PegasusBag.TRANSFORMATION_MAPPER,  Mapper.loadTCMapper( mProps.getTCMapperMode() ) );

        //write out the daxes to the directory
        File dir = new File( mBaseDir, mRequestID );
        Collection daxes;
        GetDAX getDax = new GetDAX();
        try{
            log( "Writing daxes to directory " + dir,
                 LogManager.DEBUG_MESSAGE_LEVEL );
            getDax.connect( mProps );
            daxes = getDax.get( mRequestID, dir.getAbsolutePath() );
            mLogger.logCompletion( "Writing daxes to directory " + dir,
                                   LogManager.DEBUG_MESSAGE_LEVEL);
        }
        finally{
            getDax.close();
            getDax = null;
        }

        //now rank the daxes
        Rank rank = new Rank();
        rank.initialize( mBag, (List)mSites, mRequestID );
        Collection rankings = rank.rank( daxes );

        //write out the rankings file
        File f = null;
        if( mOutputFile == null ){
            mLogger.log( "Output file not specified. Writing out ranked file in dir " + dir,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            f = new File( dir, "ranked_daxes.txt" );
        }
        else{
            f = new File( mOutputFile );
        }

        log( "Writing out the ranking file " + f, LogManager.DEBUG_MESSAGE_LEVEL );
        try{
            writeOutRankings( f, rankings );
        }catch( IOException ioe ){
            throw new RuntimeException( "Unable to write to file " + f , ioe );
        }
    }

    /**
     * Writes out the ranking to the file. If the file is null then it is written
     * out to a file named ranked_daxes.txt in the directory where the daxes
     * reside
     *
     * @param file    String
     * @param rankings Collection
     *
     * @throws IOException
     */
    protected void writeOutRankings( File file , Collection<Ranking> rankings )
        throws IOException{

        //do a sanity check on the directory for the file specified
        File dir = file.getParentFile();
        sanityCheck( dir );


        //write out the ranked daxes.
        PrintWriter pw = new PrintWriter( new FileWriter( file ) );
        int i = 1;
        for ( Iterator it = rankings.iterator(); it.hasNext() && i <= mTopNum ; i++ ) {
            pw.println( it.next() );
            pw.println( mPlannerOptions.toOptions() );
        }
        pw.close();

    }


    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck( File dir ) throws IOException{
        if ( dir.exists() ) {
            // location exists
            if ( dir.isDirectory() ) {
                // ok, isa directory
                if ( dir.canWrite() ) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException( "Cannot write to existing directory " +
                                           dir.getPath() );
                }
            } else {
                // exists but not a directory
                throw new IOException( "Destination " + dir.getPath() + " already " +
                                       "exists, but is not a directory." );
            }
        } else {
            // does not exist, try to make it
            if ( ! dir.mkdirs() ) {
                throw new IOException( "Unable to create  directory " +
                                       dir.getPath() );
            }
        }
    }



    /**
     * Loads all the properties that would be needed by the Toolkit classes.
     * Empty implementation.
     */
    public void loadProperties(){

    }

    /**
     * This method is used to print the long version of the command.
     */
    public void printLongVersion(){
        printShortHelp();
    }

    /**
     * This is used to print the short version of the command.
     */
    public void printShortVersion(){
        printShortHelp();
    }


    /**
     * This is used to print the short version of the command.
     */
    public void printShortHelp(){
        StringBuffer text = new StringBuffer();
        text.append( "\n" ).append( " $Id$ ").
             append( "\n" ).append( getGVDSVersion() ).
             append( "\n" ).append( "Usage : rank-dax [-Dprop  [..]]  -r <request id> -f <options to pegasus-plan> -d <base directory> " ).
             append( "\n" ).append( " [-s site[,site[..]]] [-o <output file>] [-t execute top t] [-v] [-h]" );

       System.out.println( text.toString() );

    }


    /**
     * It generates the LongOpt which contain the valid options that the command
     * will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid
     * options
     */
    public LongOpt[] generateValidOptions(){
        LongOpt[] longopts = new LongOpt[8];

        longopts[0]   = new LongOpt( "dir", LongOpt.REQUIRED_ARGUMENT, null, 'd' );
        longopts[1]   = new LongOpt( "sites", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[2]   = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[3]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[4]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[5]   = new LongOpt( "request-id", LongOpt.OPTIONAL_ARGUMENT, null, 'r' );
        longopts[6]   = new LongOpt( "forward", LongOpt.REQUIRED_ARGUMENT, null, 'f' );
        longopts[7]   = new LongOpt( "top", LongOpt.REQUIRED_ARGUMENT, null, 't' );

        return longopts;
    }

    /**
     * Generates a List by parsing a comma separated string.
     *
     * @param str   the comma separted String.
     *
     * @return List containing the parsed values, in case of a null string
     *             an empty List is returned.
     */
    private List generateList( String str ){
        List l = new LinkedList();

        //check for null
        if( str == null ) { return l; }

        for ( StringTokenizer st = new StringTokenizer(str,","); st.hasMoreElements(); ){
            l.add( st.nextToken().trim() );
        }

        return l;
    }


}
