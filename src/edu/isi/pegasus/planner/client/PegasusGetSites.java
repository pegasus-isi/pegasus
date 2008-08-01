package edu.isi.pegasus.planner.client;



import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactoryException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteCatalogUtil;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteInfo;
import edu.isi.pegasus.planner.catalog.site.classes.VORSVOInfo;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.toolkit.Executable;
import org.griphyn.common.util.Version;

public class PegasusGetSites extends Executable{	
    private static LogManager mLogger = LogManager.getInstance();	 
    private String vo ="";
    private String grid ="";
    private String source="";
    private SiteCatalog catalog = null;
    private String file = null;
    PegasusGetSites(){
        super();
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
            me.executeCommand( args );
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

    @Override
    public void loadProperties() {
        
    }

    @Override
    public void printLongVersion() {
             StringBuffer text = new StringBuffer();
        text.append( "\n" ).append( " $Id: PegasusGetSites.java 594 2008-07-18 18:50:50Z vahi $ ").
             append( "\n" ).append( getGVDSVersion() ).
             append( "\n" ).append( "Usage : pegasus-get-sites -s <site> -g <grid> -o <vo> " ).
             append( "\n" ).append( " [-v] [-h]" );

       System.out.println( text.toString() );
    }

    @Override
    public void printShortVersion() {
            StringBuffer text = new StringBuffer();
        text.append( "\n" ).append( " $Id: PegasusGetSites.java 594 2008-07-18 18:50:50Z vahi $ ").
             append( "\n" ).append( getGVDSVersion() ).
             append( "\n" ).append( "Usage : pegasus-get-sites -s <site> -g <grid> -o <vo> " ).
             append( "\n" ).append( " [-v] [-h]" );

       System.out.println( text.toString() );
    }

    @Override
    public void executeCommand(String[] args) {
        parseCommandLineArguments(args);
        PegasusProperties p =  PegasusProperties.nonSingletonInstance();
        if(!source.equals("VORS")){
            throw new RuntimeException("This support only VORS but the source provided is "+source);
        }
        else{
            p.setProperty( "pegasus.catalog.site", "VORS" );
        }
        if(file == null){
            //no sc path is passed using command line                                
            //sc path is not set in the properties file go to default
            File f = new File(p.getPegasusHome(), "var/sites.xml");
            file = f.getAbsolutePath();                                    
                
        }            
        
        if(vo != null){
            p.setProperty( "pegasus.catalog.site.vors.vo", vo );
        }
        if(grid != null){
            p.setProperty( "pegasus.catalog.site.vors.grid", grid );
        }
        
        try{                                    
            catalog = SiteFactory.loadInstance( p);            	
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
            catalog.load( s ); 
            List toLoad = new ArrayList( catalog.list() );
            toLoad.add( "local" );            
            //load into SiteStore from the catalog.
            for( Iterator<String> it = toLoad.iterator(); it.hasNext(); ){
                SiteCatalogEntry se = catalog.lookup( it.next() );
                if( se != null ){
                    store.addEntry( se );
                }
            }        
                 //write DAX to file
            FileWriter scFw = new FileWriter( file );
            System.out.println( "Writing out site catalog to " + file );
            store.toXML( scFw, "" );
            scFw.close();

            //test the clone method also
            System.out.println( store);
  
        }
        catch ( SiteCatalogException e ){
            e.printStackTrace();
        }   
        catch( IOException ioe ){
            ioe.printStackTrace();
        }
     
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

        Getopt g = new Getopt("pegasus-get-sites", args, "r:g:o:s:hvV", longOptions, false);
        g.setOpterr(false);

        int option = 0;
        int level = 0;
        while ( (option = g.getopt()) != -1) {
            //System.out.println("Option tag " + (char)option);
            switch (option) {

                case 'r': //base directory
                     source = g.getOptarg();
                    break;

                case 'g': //comma separated list of sites
                    grid = g.getOptarg();
                    break;

                case 'o': //the output file where the ranked list is kept
                    vo = g.getOptarg();
                    break;
                    
                case 's': //the output file where the ranked list is kept
                    file = g.getOptarg();
                    break;
                    
                case 'v': //sets the verbosity level
                    level++;
                    break;
                case 'V'://version
                    mLogger.log(getGVDSVersion(),LogManager.INFO_MESSAGE_LEVEL);
                    System.exit(0);

                case 'h':
                    printShortVersion();
                    System.exit( 0 );
                    break;

                default: //same as help
                    printShortVersion();
                    for( int i =0 ; i < args.length ; i++ )
                        System.out.println( args[i] );
                    throw new RuntimeException("Incorrect option or option usage " +
                                               option);
            }
        }
        if( level > 0 ){
            mLogger.setLevel( level );
        }
    }
    @Override
    public LongOpt[] generateValidOptions() {
          LongOpt[] longopts = new LongOpt[7];

        longopts[0]   = new LongOpt( "source", LongOpt.REQUIRED_ARGUMENT, null, 'r' );
        longopts[1]   = new LongOpt( "grid", LongOpt.REQUIRED_ARGUMENT, null, 'g' );
        longopts[2]   = new LongOpt( "vo", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[3]   = new LongOpt( "sc", LongOpt.REQUIRED_ARGUMENT, null, 's' );
        longopts[4]   = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[5]   = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[6]   = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        
        return longopts;
    }
   
}
