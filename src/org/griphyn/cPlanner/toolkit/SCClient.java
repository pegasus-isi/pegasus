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

import org.griphyn.cPlanner.classes.PoolConfig;
import org.griphyn.cPlanner.classes.PoolConfigException;
import org.griphyn.cPlanner.classes.PoolConfigParser2;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactoryException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteInfo2SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import org.griphyn.cPlanner.parser.ConfigXmlParser;

//import org.griphyn.cPlanner.poolinfo.MdsQuery;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

//import javax.naming.NamingEnumeration;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.common.PegasusProperties;
//import javax.naming.directory.SearchControls;
import edu.isi.pegasus.common.util.FactoryException;
//import javax.naming.ldap.LdapContext;

/**
 * A client to convert site catalog between different formats.
 * 
 * @author Karan Vahi
 * @author Gaurang Mehta gmehta@isi.edu
 *
 * @version $Revision$
 */
public class SCClient
    extends Executable {
    
    /**
     * The default output format.
     */
    private static String DEFAULT_OUTPUT_FORMAT = "XML3";
    
    /**
     * The XML format.
     */
    private static String XML_FORMAT = "XML";
    
    /**
     * The textual format.
     */
    private static String TEXT_FORMAT = "Text";

    private static final String XML_NAMESPACE="http://pegasus.isi.edu/schema";
    private static final String XML_VERSION="2.0";


    //private boolean mText;

    /**
     * The input files.
     */
    private List<String> mInputFiles;

    
    /**
     * The output file that is written out.
     */
    private String mOutputFile;
    
    /**
     * The output format for the site catalog.
     */
    private String mOutputFormat;
    
    /**
     * The input format for the site catalog.
     */
    private String mInputFormat;

    /**
     * The default constructor.
     */
    public SCClient() {
        super();
        
        //the output format is whatever user specified in the properties
        mOutputFormat = mProps.getPoolMode();
        mInputFormat  = SCClient.TEXT_FORMAT;
        
        //mText = false;
        
        mInputFiles = null;
        mOutputFile = null;
    }

    /**
     * Sets up the logging options for this class. Looking at the properties
     * file, sets up the appropriate writers for output and stderr.
     */
    protected void setupLogging(){
        //setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance( mProps );
        mLogger.logEventStart( "event.pegasus.sc-client", "pegasus.version",  mVersion );

    }
    /**
     * Loads all the properties
     * that would be needed
     * by the Toolkit classes
     */
    public void loadProperties() {
        
    }

    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[ 9 ];
        longopts[ 0 ] = new LongOpt( "text", LongOpt.NO_ARGUMENT, null, 't' );
        longopts[ 1 ] = new LongOpt( "files", LongOpt.REQUIRED_ARGUMENT, null,            'f' );
        longopts[ 2 ] = new LongOpt( "input", LongOpt.REQUIRED_ARGUMENT, null, 'i' );
        longopts[ 3 ] = new LongOpt( "iformat", LongOpt.REQUIRED_ARGUMENT, null, 'I' );
        longopts[ 4 ] = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null, 'o' );
        longopts[ 5 ] = new LongOpt( "oformat", LongOpt.REQUIRED_ARGUMENT, null, 'O' );
        longopts[ 6 ] = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[ 7 ] = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[ 8 ] = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );

        return longopts;

    }

    /**
     * Call the correct commands depending on options.
     * @param opts Command options
     */

    public void executeCommand( String[] opts ) throws IOException {
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt( "SCClient", opts, "lthvVi:I:o:O:f:",
            longOptions, false );

        int option = 0;
        int noOfOptions = 0;
        int level = 0;
        while ( ( option = g.getopt() ) != -1 ) {
            switch ( option ) {
                case 't': //text 
                    //mText = true;
                    mOutputFormat = SCClient.TEXT_FORMAT;
                    break;

                case 'f': //files
                    StringTokenizer st = new StringTokenizer( g.getOptarg(), "," );
                    mInputFiles = new ArrayList( st.countTokens() );
                    while ( st.hasMoreTokens() ) {
                        mInputFiles.add( st.nextToken() );
                    }
                    break;

                case 'i': //input
                    StringTokenizer str = new StringTokenizer( g.getOptarg(), "," );
                    mInputFiles = new ArrayList( str.countTokens() );
                    while ( str.hasMoreTokens() ) {
                        mInputFiles.add( str.nextToken() );
                    }
                    break;
                    
                case 'I': //iformat
                    mInputFormat = g.getOptarg();
                    break;
                    
                case 'o': //output
                    mOutputFile = g.getOptarg();
                    break;

                
                case 'O': //oformat
                    mOutputFormat = g.getOptarg();
                    break;
                    
                case 'h': //help
                    printLongVersion();
                    System.exit( 0 );
                    break;

                case 'V': //version
                    mLogger.log( getGVDSVersion(), LogManager.INFO_MESSAGE_LEVEL);
                    System.exit( 0 );
                    break;
                    
                    /*
                case 'l': // Precedence for local or remote
                    mLocalPrec = true;
                    break;
                     */
                    
                case 'v': //Verbose mode
                    level++;
                    break;

                default:
                    mLogger.log( "Unrecognized Option : " + (char)option,
                                LogManager.FATAL_MESSAGE_LEVEL );
                    printShortVersion();
                    System.exit( 1 );
            }
        }
        if(level > 0){
            //set the logging level only if -v was specified
            //else bank upon the the default logging level
            mLogger.setLevel(level);
        }
        
        String result = this.parseInputFiles( mInputFiles, mInputFormat, mOutputFormat );
        //write out the result to the output file
        this.toFile( mOutputFile, result );
	
    }


    /**
     * Parses the input files in the input format and returns a String in the 
     * output format.
     * 
     * @param inputFiles      list of input files that need to be converted
     * @param inputFormat     input format of the input files
     * @param outputFormat    output format of the output file
     * 
     * @return  String in output format
     * 
     * @throws java.io.IOException
     */
    public String parseInputFiles( List<String> inputFiles, String inputFormat, String outputFormat ) throws IOException{
	//sanity check
        if ( inputFiles == null || inputFiles.isEmpty() ){
            throw new IOException( "Input files not specified. Specify the --input option" );
        }
        
        mLogger.log( "Input  format detected is " + inputFormat , LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "Output format detected is " + outputFormat , LogManager.DEBUG_MESSAGE_LEVEL );
        
        //check if support for backward compatibility applies
        boolean backwardCompatibility =  mInputFormat.equals( SCClient.TEXT_FORMAT ) &&
                                         mOutputFormat.equals( SCClient.XML_FORMAT ) ;
        
        if( backwardCompatibility ){
            return parseInputFilesForBackwardCompatibility( inputFiles, inputFormat, outputFormat );
        }
        
        //sanity check for output format
        if ( !outputFormat.equals( SCClient.DEFAULT_OUTPUT_FORMAT )){
            throw new RuntimeException( "Only XML3 output format is currently supported");
        }
        
        SiteStore result = new SiteStore();
        for( String inputFile : inputFiles ){
            //switch on input format.
            if( inputFormat.equals( "XML" ) ){
                SiteCatalog catalog = null;
        
                /* load the catalog using the factory */
                try{
                    mProps.setProperty( "pegasus.catalog.site.file", inputFile );
                    mProps.setProperty( SiteCatalog.c_prefix, mInputFormat );
                    catalog = SiteFactory.loadInstance( mProps );
                
                    /* load all sites in site catalog */
                    List s = new ArrayList(1);
                    s.add( "*" );
                    mLogger.log( "Loaded  " + catalog.load( s ) + " number of sites ", LogManager.DEBUG_MESSAGE_LEVEL );
        
                    /* query for the sites, and print them out */
                    mLogger.log( "Sites loaded are "  + catalog.list( ) , LogManager.DEBUG_MESSAGE_LEVEL );
                    for( String site : catalog.list() ){
                        result.addEntry( catalog.lookup( site ) );
                    }
                }
                finally{
                    /* close the connection */
                    try{
                        catalog.close();
                    }catch( Exception e ){}
                }
            }//end of input format xml
            else if ( inputFormat.equals( "Text" ) ){
                //do a two step process.
                //1. convert to PoolConfig
                //2. convert to SiteCatalogEntry
                PoolConfig config = this.getTextToPoolConfig( inputFile );
                
                //iterate through each entry
                for( Iterator it = config.getSites().values().iterator(); it.hasNext(); ){
                    SiteInfo s = (SiteInfo)it.next();
                    
                    //convert and add to site store
                    result.addEntry( SiteInfo2SiteCatalogEntry.convert( s , mLogger ) );
                }
            }//end of input format Text
        }//end of iteration through input files.
       
        return result.toXML();
    }

    /**
     * Parses the input files in the input format and returns a String in the old XML
     * output format.
     * 
     * @param inputFiles      list of input files that need to be converted
     * @param inputFormat     input format of the input files
     * @param outputFormat    output format of the output file
     * 
     * @return  String in output format ( old XML )
     * 
     * @throws java.io.IOException
     */
    private String parseInputFilesForBackwardCompatibility( List<String> inputFiles,
                                                            String inputFormat, 
                                                            String outputFormat ) {
        
        PoolConfig result = new PoolConfig();
        for( String inputFile : inputFiles ){
            PoolConfig config = this.getTextToPoolConfig( inputFile );
            result.add( config );    
        }
        return this.toXML( result );
    }

    /**
     * Returns the short help.
     * 
     * 
     */
    public void printShortVersion() {
        String text =
            "\n $Id$ " +
            "\n " + getGVDSVersion() +
            "\n Usage: sc-client [-Dprop  [..]]  -i <list of input files> -o <output file to write> " +
            "\n        [-I input format] [-O <output format> [-v] [-V] [-h]" ;

        mLogger.log( text,LogManager.ERROR_MESSAGE_LEVEL );
    }

    public void printLongVersion() {
        String text =
           "\n $Id $ " +
           "\n " + getGVDSVersion() +
           "\n sc-client - Parses the site catalogs in old format ( Text and XML3 ) and generates site catalog in new format ( XML3 )"  +
           "\n " +
           "\n Usage: sc-client [-Dprop  [..]]  --input <list of input files> --output <output file to write> " +
            "\n        [--iformat input format] [--oformat <output format> [--verbose] [--Version] [--help]" +
            "\n" +   
            "\n" +
            "\n Mandatory Options " +
            "\n" +
            "\n -i |--input      comma separated list of input files to convert " +
            "\n -o |--output     the output file to which the output needs to be written to." +
            "\n" +
            "\n" +
            "\n Other Options " +
            "\n" +
            "\n -I |--iformat    the input format for the files . Can be [XML , Text] "  + 
            "\n -O |--oformat    the output format of the file. Usually [XML3] " +
            "\n -v |--verbose       increases the verbosity of messages about what is going on" +
            "\n -V |--version       displays the version of the Pegasus Workflow Planner" +
            "\n -h |--help          generates this help." +
            "\n" + 
            "\n" + 
            "\n Deprecated Options " +
            "\n" + 
            "\n --text | -t        To convert an xml site catalog file to the multiline site catalog file." +
            "\n                    Use --iformat instead " + 
            "\n" +
            "\n --files | -f  The local text site catalog file|files to be converted to " +
            "\n                    xml or text. This file needs to be in multiline textual " +
            "\n                    format not the single line or in xml format if converting " +
            "\n                    to text format. See $PEGASUS_HOME/etc/sample.sites.txt. " +
            "\n" +
            "\n" +
            "\n Example Usage " +
            "\n" + 
            "\n sc-client  -i sites.xml -I XML -o sites.xml.new  -O XML3 -vvvvv" +
            "\n" +
            "\n" +
            "\n Deprecated Usage . Exists only for backward compatibility " +
            "\n" + 
            "\n sc-client --files sites.txt --output sites.xml" ;

        mLogger.log( text,LogManager.INFO_MESSAGE_LEVEL );

    }

    /**
     * Generates the old site catalog object reading in from text file.
     * 
     * 
     * @param file text file to parse.
     * 
     * @return PoolConfig
     */
    public PoolConfig getTextToPoolConfig( String file ) {
        PoolConfig result = new PoolConfig();
        try {
            
            mLogger.log( "Reading " + file, LogManager.INFO_MESSAGE_LEVEL);
            PoolConfigParser2 p = new PoolConfigParser2( new FileReader( file ) );
            result.add(p.parse());
            mLogger.log( "Reading " + file + " -DONE",
                             LogManager.INFO_MESSAGE_LEVEL);
        } catch ( PoolConfigException pce ) {
           mLogger.log( file + ": " + pce.getMessage() ,
                             LogManager.ERROR_MESSAGE_LEVEL);
           mLogger.log(
                    " ignoring rest, skipping to next file",
                   LogManager.ERROR_MESSAGE_LEVEL );
        } catch ( IOException ioe ) {
           mLogger.log( file + ": " + ioe.getMessage() ,
                        LogManager.ERROR_MESSAGE_LEVEL);
           mLogger.log("ignoring rest, skipping to next file",
                           LogManager.ERROR_MESSAGE_LEVEL );
        } catch ( Exception e ) {
           mLogger.log( file + ": " + e.getMessage(),
                            LogManager.ERROR_MESSAGE_LEVEL );
           mLogger.log("ignoring rest, skipping to next file",
                           LogManager.ERROR_MESSAGE_LEVEL );
        }
        return result;
    }
    

    /**
     * Returns the XML description of the  contents of <code>PoolConfig</code>
     * object passed, conforming to pool config schema found at
     * http://pegasus.isi.edu/schema/sc-2.0.xsd.
     *
     * @param cfg the <code>PoolConfig</code> object whose xml description is
     *            desired.
     *
     * @return the xml description.
     */
    public String toXML( PoolConfig cfg ) {
        String output = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        output += "<!--Generated " + new Date().toString() + "-->\n";
        output += "<!--Generated by " + System.getProperty( "user.name" ) +
            " [" +
            System.getProperty( "user.country" ) + "] " + "-->\n";
        output += "<sitecatalog";
        output += " xmlns=\""+XML_NAMESPACE+"/sitecatalog\"";
        output +=
            " xsi:schemaLocation=\""+XML_NAMESPACE+"/sitecatalog " +
            XML_NAMESPACE+"/sc-"+XML_VERSION+".xsd\"";
        output += " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
        output += " version=\""+XML_VERSION+"\">\n";
        output += cfg.toXML();
        output += "</sitecatalog>";

        return output;
    }

    /**
     * Returns the String description of the  contents of <code>PoolConfig</code>
     * object passed.
     *
     * @param cfg the <code>PoolConfig</code> object whose description is
     *            desired.
     *
     * @return the String description.
     */
    public String toMultiLine( PoolConfig cfg ) {
        String output = "#Text version of site catalog\n";
        output += "#Generated by SCClient\n";
        output += cfg.toMultiLine();
        output += "\n";
        return output;
    }

    /**
     * Writes out to a file, a string.
     *
     * @param filename  the fully qualified path name to the file.
     * @param output    the text that needs to be written to the file.
     *
     * @throws IOException
     */
    public void toFile( String filename, String output ) throws IOException {
        if( filename == null ){
            throw new IOException( "Please specify a file to write the output to using --output option ");
        }
        
        File outfile = new File( filename );
        
        PrintWriter pw = new PrintWriter( new BufferedWriter( new FileWriter(
            outfile ) ) );
        pw.println( output );
        pw.close();
        mLogger.log( "Written out the converted file to " + filename, LogManager.INFO_MESSAGE_LEVEL );
    }

    public static void main( String[] args ) throws Exception {
        
        SCClient me = new SCClient();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime  = -1;

        try{
             me.executeCommand( args );
        }
        catch ( IOException ioe ){
            me.log( ioe.getMessage(), LogManager.FATAL_MESSAGE_LEVEL);
            result = 1;
        }
        catch ( FactoryException fe){
            me.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
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
        
        me.log( "Exiting with exitcode " + result, LogManager.DEBUG_MESSAGE_LEVEL );
        me.mLogger.logEventCompletion();
        System.exit(result);
        
    }

    
}
