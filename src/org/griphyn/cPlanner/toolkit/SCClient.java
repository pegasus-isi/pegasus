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

package org.griphyn.cPlanner.toolkit;

import org.griphyn.cPlanner.classes.PoolConfig;
import org.griphyn.cPlanner.classes.PoolConfigException;
import org.griphyn.cPlanner.classes.PoolConfigParser2;

import org.griphyn.cPlanner.common.LogManager;

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
import java.util.StringTokenizer;

//import javax.naming.NamingEnumeration;
//import javax.naming.directory.SearchControls;
//import javax.naming.ldap.LdapContext;

/**
 * This client generates a xml poolconfig file by querying the MDS or local
 * multiline poolconfig files.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 *
 * @version $Revision$
 */
public class SCClient
    extends Executable {



    private boolean mText;

    private ArrayList mLocalPoolConfig;

    private String mOutputXML;

    /**
     * The data class containing the contents of the site catalog.
     */
    private PoolConfig mConfig;

    private boolean mLocalPrec;

    public SCClient() {
        super();
	//    mGIISPort = 2135;
        mText = false;
        mLocalPoolConfig = null;
        mOutputXML = null;
        mConfig = null;
        mLocalPrec = false;
        mConfig = new PoolConfig();
    }

    /**
     * Loads all the properties
     * that would be needed
     * by the Toolkit classes
     */
    public void loadProperties() {
	//        mGIISHost = mProps.getGIISHost();
        //mGIISDN = mProps.getGIISDN();
    }

    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[7 ];
        longopts[ 0 ] = new LongOpt( "local", LongOpt.NO_ARGUMENT, null, 'l' );
        longopts[ 1 ] = new LongOpt( "text", LongOpt.NO_ARGUMENT, null, 't' );
        longopts[ 2 ] = new LongOpt( "files", LongOpt.REQUIRED_ARGUMENT, null,
            'f' );
        longopts[ 3 ] = new LongOpt( "output", LongOpt.REQUIRED_ARGUMENT, null,
            'o' );
        longopts[ 4 ] = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[ 5 ] = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[ 6 ] = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );

        return longopts;

    }

    /**
     * Call the correct commands depending on options.
     * @param opts Command options
     */

    public void executeCommand( String[] opts ) {
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt( "SCClient", opts, "lthvVo:f:",
            longOptions, false );

        int option = 0;
        int noOfOptions = 0;
        int level = 0;
        while ( ( option = g.getopt() ) != -1 ) {
            switch ( option ) {
                case 't': //text or xml
                    mText = true;

                    break;

                case 'f': //local pool config file
                    StringTokenizer st = new StringTokenizer( g.getOptarg(),
                        "," );
                    mLocalPoolConfig = new ArrayList( st.countTokens() );
                    while ( st.hasMoreTokens() ) {
                        mLocalPoolConfig.add( st.nextToken() );
                    }
                    break;

                case 'o': //output
                    mOutputXML = g.getOptarg();
                    break;

                case 'h': //help
                    printLongVersion();
                    System.exit( 0 );
                    break;

                case 'V': //version
                    mLogger.log( getGVDSVersion(), LogManager.INFO_MESSAGE_LEVEL);
                    System.exit( 0 );
                    break;
                case 'l': // Precedence for local or remote
                    mLocalPrec = true;
                    break;
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
	//        if ( mGIISHost != null && mGIISDN == null ) {
	    //            mLogger.log("The giis dn is missing. "+
	//              "\nYou need to define vds.giis.dn in the properties file",
	//               LogManager.FATAL_MESSAGE_LEVEL );
	//   printShortVersion();
	//   System.exit( 1 );

	//        }

	//        if ( mGIISHost == null && mLocalPoolConfig == null ) {
	//  mLogger.log(
	//       "The giis host as well as the local pool config file is missing." +
	//       "\nYou need to define vds.giis.host and vds.giis.dn in the properties file" +
	//       "\nor provide a local config file to generate the xml pool config file",
	//     LogManager.FATAL_MESSAGE_LEVEL );
	//  printShortVersion();
	//  System.exit( 1 );
	//
	//        } else {
	try {
	    generatePoolConfig();
	} catch ( Exception e ) {
	    e.printStackTrace();
	}
    }
    //    }

    public void generatePoolConfig() throws Exception {
        if ( mText ) {
            //convert the xml file to text file.
            if ( mLocalPoolConfig != null ) {
                ConfigXmlParser p = new ConfigXmlParser( mProps );
                p.startParser( ( String ) mLocalPoolConfig.get( 0 ) );
                mConfig = p.getPoolConfig();
            } else {
                mLogger.log("Provide the xml pool config file with --poolconfig option",
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
        }// else {
         //   if ( mGIISHost != null ) {
	//      StringTokenizer st = new StringTokenizer( mGIISHost, ":" );
	//
	//      if ( st.countTokens() < 2 ) {
	//          mGIISHost = st.nextToken().trim();
	//      } else {
	//          mGIISHost = st.nextToken();
	//          mGIISPort = ( new Integer( st.nextToken() ).intValue() );
	//      }
	//  }
	//
	//  if ( mLocalPrec ) {
	//      if ( mLocalPoolConfig != null ) {
	//          mConfig = getLocalConfigInfo( mLocalPoolConfig );
	//              }
//      if ( mGIISHost != null ) {
	//          mConfig = getMdsInfo( mGIISHost, mGIISPort,
	//              mGIISDN );
	//      }
	//  } else {
	//      if ( mGIISHost != null ) {
	//          mConfig = getMdsInfo( mGIISHost, mGIISPort,
	//              mGIISDN );
	//      }
	//      if ( mLocalPoolConfig != null ) {
	//          mConfig = getLocalConfigInfo( mLocalPoolConfig );
	//      }
	//
	//  }
        //}
        if ( mConfig != null ) {
            if ( mText && mOutputXML == null ) {
                //not sure about this
                System.out.println( this.toMultiLine( mConfig ) );
            } else if ( !mText && mOutputXML == null ) {
                System.out.println( this.toXML( mConfig ) );
            } else if ( mText && mOutputXML != null ) {
                toFile( mOutputXML, this.toMultiLine( mConfig ) );
                mLogger.log( "Written text output to file :" +
                    mOutputXML, LogManager.INFO_MESSAGE_LEVEL );
                System.exit( 0 );
            } else {
                toFile( mOutputXML, this.toXML( mConfig ) );
                mLogger.log( "Written xml output to file : " +
                    mOutputXML,LogManager.INFO_MESSAGE_LEVEL);
                System.exit( 0 );
            }

        } else {
            throw new Exception(
                "Error: Something bad happened the config data is empty" );
        }
    }

    public void printShortVersion() {
        String text =
            "\n " + this.getGVDSVersion() +
            "\n Usage :sc-client  [-f <list of files>] " +
            "\n  [-o <output filename>] [-l] [-t] [-v] [-V] " +
            "\n Type sc-client -h for more details" +
            "\n" +
            "\n Usage :sc-client  [--files <list of files>] " +
            "\n [--local] [--text] [--output <output filename>] [--verbose] [--version]" +
            "\n Type sc-client --help for more help";

        mLogger.log( text,LogManager.ERROR_MESSAGE_LEVEL );

    }

    public void printLongVersion() {
        String text =
            "\n" + this.getGVDSVersion() +
            "\n sc-client - this is used to write the xml site catalog file" +
            "\n from a local text config file. " +
            "\n Usage: sc-client  [OPTIONS]...." +

            "\n\n Mandatory Options " +
            "\n" +
            "\n --text | -t        To convert an xml site catalog file to the multiline site catalog file." +
            "\n" +
            "\n --files | -f  The local text site catalog file|files to be converted to " +
            "\n                    xml or text. This file needs to be in multiline textual " +
            "\n                    format not the single line or in xml format if converting " +
            "\n                    to text format. See $PEGASUS_HOME/etc/sample.sites.txt. " +
            "\n" +
            "\n --output | -o      The name of the xml/text file to which you want the ouput " +
            "\n                    written to. Default it writes to standard out." +
            "\n" +
            "\n\n Other Options " +
            "\n" +
            "\n -v | --verbose   increases the verbosity level." +
            "\n" +
            "\n --version | -V     Displays the version number of PEGASUS. " +
            "\n" +
            "\n --help   | -h      Generates this help." +
            "\n" +
            "\n\n Example Usages " +
            "\n sc-client --files sites.txt --output sites.xml" +
            "\n" +
            "\n sc-client --files sites.txt,sites2.txt " +
            "\n --output sites.xml" +
            "\n" +
            "\n sc-client --files sites.xml --text --output sites.txt \n";

        mLogger.log( text,LogManager.INFO_MESSAGE_LEVEL );

    }

    /**
     * This method obtains the MDS information and stuffs it into the poolconfig object.
     * @param host The host name for the giis.
     * @param port The port for the Giis.
     * @param baseDN The base dn for the Giis.
     * @return PoolConfig
     * @throws java.lang.Exception
     */
    //for PEGASUS Release May 2007
    //        public PoolConfig getMdsInfo( String host, int port, String baseDN ) throws
    //    Exception {
    //    String message = "Quering GIIS on host " + host + ":" + port;
    //   mLogger.log( message,LogManager.INFO_MESSAGE_LEVEL);
    //   String filtergvds = "(objectclass=Gvds-Pool-Info)";
    //  String filtergram =
    //      "(|(objectclass=MdsServiceGram)(objectclass=MdsGramJobQueue))";
    //
    //  //  sk added a filter for getting jobmanagersubnet and for bandwidth
    //  String filtergvdsBandwidth =
    //      "(objectclass=gridftp-pair-bandwidth-info)";
    //
    //  MdsQuery mds = new MdsQuery( host, port );
    //
    //       /**
    //   * create a Ldap Context
    //  */
    //
    //  LdapContext ctx = mds.connectMds();
    //
    //  /**
    //   * Specify the Search controls
    //   * Enable Subtre searching scope.
    // */
    //
    //  SearchControls constraints = new SearchControls();
    //  constraints.setSearchScope( SearchControls.SUBTREE_SCOPE );
    //
    //  NamingEnumeration resultsgvds, resultsgram, resultsgvdsBandwidth;
    //  /**
    //   * Search the context for the required objects.
    //   */
    //  mLogger.log( "Querying GVDS entries in the GIIS ",
    //               LogManager.DEBUG_MESSAGE_LEVEL);
    //  resultsgvds = ctx.search( baseDN, filtergvds, constraints );
    //  mLogger.logCompletion("Querying GVDS entries in the GIIS ",
    //                        LogManager.DEBUG_MESSAGE_LEVEL);
    //  mLogger.log( "Querying Gram entries in the GIIS",
    //               LogManager.DEBUG_MESSAGE_LEVEL);
    //  resultsgram = ctx.search( baseDN, filtergram, constraints );
    //  mLogger.logCompletion("Querying Gram entries in the GIIS",
    //                        LogManager.DEBUG_MESSAGE_LEVEL );
    // //TODO:sk added a search
    //  //commented this because subnet information is no more required
    //  //resultssubnet = ctx.search(baseDN,filtersubnet,constraints);
    //  resultsgvdsBandwidth = ctx.search( baseDN, filtergvdsBandwidth,
    //      constraints );
    //  mLogger.logCompletion(message,LogManager.DEBUG_MESSAGE_LEVEL );
    //
    //  /**
    //   * Close the Ldap Context
    //   */
    //  ctx.close();
    //
    //  /**
    //     * Store the results from the MDS into the dataclasses
    //     */
    //    message = "Storing GVDS entries from the GIIS";
    //   mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);
    //   mConfig = mds.StoreGvdsMdsInfo( resultsgvds, baseDN, mConfig );
    //   mLogger.logCompletion( message, LogManager.DEBUG_MESSAGE_LEVEL );
    //   message = "Storing Gram entries from the GIIS";
    //   mLogger.log( message,LogManager.DEBUG_MESSAGE_LEVEL);
    //   mConfig = mds.StoreGvdsMdsInfo( resultsgram, baseDN, mConfig );
    //   mLogger.logCompletion(message,LogManager.DEBUG_MESSAGE_LEVEL);
    //   //  mConfig = mds.StoreGvdsMdsInfo(resultsqueue, baseDN, mConfig);
    //   message = "Quering GIIS on host " + host + ":" + port;
    //   mLogger.logCompletion( message,LogManager.INFO_MESSAGE_LEVEL );
    //
    //   //sk storing the result into the dataclass
    //   mConfig = mds.StoreGvdsMdsInfo( resultsgvdsBandwidth, baseDN, mConfig );
    //
    //   return mConfig;
    //}


    public PoolConfig getLocalConfigInfo( ArrayList localpoolconfig ) {
        for ( int i = 0; i < localpoolconfig.size(); i++ ) {
            String filename = null;
            try {
                filename = ( String ) localpoolconfig.get( i );
                mLogger.log( "Reading " + filename, LogManager.INFO_MESSAGE_LEVEL);
                PoolConfigParser2 p = new PoolConfigParser2( new FileReader(
                    filename ) );
                mConfig.add(p.parse());
                mLogger.logCompletion("Reading " + filename,
                                      LogManager.INFO_MESSAGE_LEVEL);
            } catch ( PoolConfigException pce ) {
                mLogger.log( filename + ": " + pce.getMessage() ,
                             LogManager.ERROR_MESSAGE_LEVEL);
                mLogger.log(
                    " ignoring rest, skipping to next file",
                   LogManager.ERROR_MESSAGE_LEVEL );
            } catch ( IOException ioe ) {
                mLogger.log( filename + ": " + ioe.getMessage() ,
                             LogManager.ERROR_MESSAGE_LEVEL);
                mLogger.log("ignoring rest, skipping to next file",
                           LogManager.ERROR_MESSAGE_LEVEL );
            } catch ( Exception e ) {
                mLogger.log( filename + ": " + e.getMessage(),
                            LogManager.ERROR_MESSAGE_LEVEL );
                mLogger.log("ignoring rest, skipping to next file",
                           LogManager.ERROR_MESSAGE_LEVEL );
            }
        }
        return mConfig;
    }

    /**
     * Returns the XML description of the  contents of <code>PoolConfig</code>
     * object passed, conforming to pool config schema found at
     * http://www.griphyn.org/chimera/gvds-poolcfg-1.5.xsd.
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
        output += " xmlns=\"http://pegasus.isi.edu/schema/sitecatalog\"";
        output +=
            " xsi:schemaLocation=\"http://pegasus.isi.edu/schema/sitecatalog " +
            " http://pegasus.isi.edu/schema/sc-2.0.xsd\"";
        output += " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
        output += " version=\"2.0\">\n";
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
        File outfile = new File( filename );
        PrintWriter pw = new PrintWriter( new BufferedWriter( new FileWriter(
            outfile ) ) );
        pw.println( output );
        pw.close();

    }

    public static void main( String[] args ) throws Exception {
        SCClient client = new SCClient();
        client.executeCommand( args );
    }

}
