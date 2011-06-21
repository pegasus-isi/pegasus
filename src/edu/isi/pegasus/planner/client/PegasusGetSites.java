/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package edu.isi.pegasus.planner.client;



import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.impl.OSGMM;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactoryException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.util.Version;


import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * The client that replaces the perl based pegasus-get-sites. 
 * It generates a Site Catalog by querying VORS.
 * 
 * @author Atul Kumar
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusGetSites extends Executable{	
    
    private String mVO = null;
    private String mGrid = null;
    
    /**
     * The default source to query for generating a site catalog.
     */
    private String mSource="OSGMM";
    
    private SiteCatalog mCatalog = null;
    private String mSCFile = null;
    private String mPropertiesFilename;
    
    /**
     * The default constructor.
     */
    public PegasusGetSites(){
        super();
    }
    
    /**
     * Initialize the PegasusGetSites object 
     * @param opts the command line argument passed to the PegasusGetSites
     */
     
    public void intialize(String [] opts){
    	super.initialize(opts);
    	mLogMsg = new String();
        mVersion = Version.instance().toString();
    }

    /**
     * The main program
     * 
     * @param args
     */
    public static void main( String[] args ){        
    	PegasusGetSites me = new PegasusGetSites();
    	int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;

        try{
        	me.initialize(args);
            me.executeCommand( );
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
        me.mLogger.logEventCompletion();
        System.exit( result );
    }

    /**
     * An empty implementation.
     */
    public void loadProperties() {
        
    }

    /**
     * Prints out the long help.
     */
    public void printLongVersion() {
        StringBuffer text = new StringBuffer();
        text.append( "\n" ).append( "$Id$ ").
             append( "\n" ).append( getGVDSVersion() ).
             append( "\n" ).append( "Usage : pegasus-get-sites --source <source> --grid <grid> --vo <vo> --sc <filename> --properties <properties file>" ).
             append( "\n" ).append( "[--conf <path to property file>] [-v] [-h]" ).
             append( "\n" ).
             append( "\n Mandatory Options " ).
             append( "\n  --source     the source to query for information. Defaults to OSGMM" ).
             append( "\n" ). 
             append( "\n Other Options  " ).
             append( "\n -g |--grid       the grid for which to generate the site catalog ").
             append( "\n -o |--vo         the virtual organization to which the user belongs " ).
             append( "\n -s |--sc         the path to the created site catalog file" ).
             append( "\n -p |--properties the properties file to be created" ).
             append( "\n -c |--conf       path to  property file").
             append( "\n -v |--verbose    increases the verbosity of messages about what is going on" ).
             append( "\n -V |--version    displays the version of the Pegasus Workflow Management System" ).
             append( "\n -h |--help       generates this help." );          
             
       System.out.println( text.toString() );
    }

    /**
     * The short help version.
     */
    public void printShortVersion() {
            StringBuffer text = new StringBuffer();
        text.append( "\n" ).append( "$Id$ ").
             append( "\n" ).append( getGVDSVersion() ).
             append( "\n" ).append( "Usage : pegasus-get-sites -source <site> -g <grid> -o <vo> -s <filename> -p <filename>" ).
             append( "\n" ).append( "[-c <path to property file>] [-v] [-h]" );
        
       System.out.println( text.toString() );
    }

    /**
     * Executes the command on the basis of the command line options passed.
     * 
     * @param args
     */
    public void executeCommand() {
        parseCommandLineArguments(getCommandLineOptions());
        PegasusProperties p =  PegasusProperties.getInstance(lookupConfProperty(getCommandLineOptions(), 'c'));
        
        p.setProperty( "pegasus.catalog.site", mSource );
        
        if(mSCFile == null){
            //no sc path is passed using command line                                
            //sc path is not set in the properties file go to default
            File f = new File(p.getPegasusHome(), "var/sites.xml");
            mSCFile = f.getAbsolutePath();
                
        }            
        
        //pass on the VO and Grid information as properties
        //to site catalog back end.
        if(mVO != null  ){
            p.setProperty( getPropertyKey( mSource, "vo" ), mVO  );
        }
        if(mGrid != null  ){
            p.setProperty( getPropertyKey( mSource, "grid" ), mGrid  );
        }
        
        try{                                    
            mCatalog = SiteFactory.loadInstance( p);            	
        }
        catch ( SiteFactoryException e ){
            System.out.println(  e.convertException() );
            System.exit( 2 );
        }
        SiteStore store = new SiteStore();
        /* load all sites in site catalog */
        try{        	                   
            List s = new ArrayList(1);
            s.add( "*" );

            
            mCatalog.load( s ); 
            List toLoad = new ArrayList( mCatalog.list() );
            toLoad.add( "local" );            
            //load into SiteStore from the catalog.
            int num = 0;
            for( Iterator<String> it = toLoad.iterator(); it.hasNext(); ){
                SiteCatalogEntry se = mCatalog.lookup( it.next() );
                if( se != null ){
                    store.addEntry( se );
                    num++;
                }
            }        
                 
            mLogger.log( "Loaded " + num + " sites ", LogManager.INFO_MESSAGE_LEVEL );
            //write DAX to file
            FileWriter scFw = new FileWriter( mSCFile  );
            mLogger.log( "Writing out site catalog to " + new File( mSCFile ).getAbsolutePath() ,
                         LogManager.CONSOLE_MESSAGE_LEVEL );
            store.toXML( scFw, "" );
            scFw.close();
           
  
            //generate the SRM properties file only if
            //interfacing with OSGMM.
            if( mCatalog instanceof OSGMM ){
                Properties properties =  ((OSGMM)mCatalog).generateSRMProperties();
                mLogger.log( "Number of SRM Properties retrieved " + properties.entrySet().size() ,
                             LogManager.CONSOLE_MESSAGE_LEVEL );
                mLogger.log( properties.toString(),
                             LogManager.DEBUG_MESSAGE_LEVEL );

                File file = ( mPropertiesFilename == null )?
                            //default one in the working directory
                            //create a temporary file in directory
                            File.createTempFile( "pegasus.", ".properties", new File( "." ) ):
                            new File( mPropertiesFilename );
                
                OutputStream os = new FileOutputStream( file );
                mLogger.log( "Writing out properties to " + file.getAbsolutePath() ,
                             LogManager.CONSOLE_MESSAGE_LEVEL );

                properties.store( os, "Pegasus SRM Properties" );
                os.close();
            }

        }
        catch ( SiteCatalogException e ){
            e.printStackTrace();
        }   
        catch( IOException ioe ){
            ioe.printStackTrace();
        }
        
    }
    
    /**
     * Returns the full name of the property key with the appropriate prefix
     * 
     * @param source  the source i.e type of site catalog 
     * @param key     the basename of the key
     *
     * @return the property key.
     */
    protected String getPropertyKey( String source, String key ){
        //"pegasus.catalog.site.vors.grid"
        String lSource = source.toLowerCase();
        
        StringBuffer property = new StringBuffer();
        property.append( "pegasus.catalog.site" ).append( "." ).
                 append( lSource ).append( "." ).
                 append( key );
        
        return property.toString();
                
    }
    
    /**
     * Sets up the logging options for this class. Looking at the properties
     * file, sets up the appropriate writers for output and stderr.
     */
    protected void setupLogging(){
        //setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance( mProps );
        mLogger.logEventStart( "event.pegasus.pegasus-get-sites", "pegasus.version",  mVersion );

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

        Getopt g = new Getopt("pegasus-get-sites", args, "1:g:o:s:p:c:hvV", longOptions, false);
        g.setOpterr(false);

        int option = 0;
        int level = 0;
        while ( (option = g.getopt()) != -1) {
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case '1': //--source
                     mSource = g.getOptarg();
                    break;

                case 'g': //--grid
                    mGrid = g.getOptarg();
                    break;

                case 'o': //--vo
                    mVO = g.getOptarg();
                    break;
                    
                case 's': //--sc
                    mSCFile = g.getOptarg();
                    break;

                case 'p': //--properties
                    mPropertiesFilename = g.getOptarg();
                    break;
                    
                case 'c': // conf
                	//do nothing
                	break;

                case 'v': //--verbose
                    level++;
                    break;
                case 'V'://--version
                    mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
                    System.exit(0);

                case 'h'://--help
                    printLongVersion();
                    System.exit( 0 );
                    break;

                default: //same as help
                    printShortVersion();
                    for( int i =0 ; i < args.length ; i++ )
                        System.out.println( args[i] );
                    throw new RuntimeException("Incorrect option or option usage " +
                    							(char)g.getOptopt());
            }
        }

        if(level >0){
        	mLogger.setLevel( level );
        }
        else{
        	mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        }
        
        if(mSource == null || mSource.isEmpty()){
        	 mLogger.log("Please provide the source to query for information",LogManager.ERROR_MESSAGE_LEVEL);
             this.printShortVersion();
             System.exit(1);
        }
    }
    
    /**
     * Generates valid LongOpts.
     * 
     * @return  LongOpt[]
     */
    public LongOpt[] generateValidOptions() {
          LongOpt[] longopts = new LongOpt[9];

        longopts[0]   = new LongOpt( "source", LongOpt.REQUIRED_ARGUMENT, null, '1' );
        longopts[1]   = new LongOpt( "grid", LongOpt.REQUIRED_ARGUMENT, null, 'g' );
        longopts[2]   = new LongOpt( "vo", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[3]   = new LongOpt( "sc", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[4]   = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[5]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[6]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[7]   = new LongOpt( "properties", LongOpt.REQUIRED_ARGUMENT, null, 'p' );
        longopts[8]   = new LongOpt( "conf", LongOpt.REQUIRED_ARGUMENT, null, 'c' );
        
        return longopts;
    }
   
}
