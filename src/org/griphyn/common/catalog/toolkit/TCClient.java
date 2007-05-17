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
package org.griphyn.common.catalog.toolkit;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.transformation.TransformationFactory;
import org.griphyn.common.catalog.transformation.TransformationFactoryException;

import org.griphyn.common.catalog.transformation.client.TCAdd;
import org.griphyn.common.catalog.transformation.client.TCDelete;
import org.griphyn.common.catalog.transformation.client.TCQuery;

import org.griphyn.common.util.Version;
import org.griphyn.common.util.FactoryException;

import java.util.HashMap;
import java.util.Map;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

/**
 * A commom client to add, modify, delete, query any Transformation Catalog
 * implementation.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TCClient {

    public String classname;

    private int add = 0;

    private int query = 0;

    private int delete = 0;

    private int bulk = 0;

    private int islfn = 0;

    private int ispfn = 0;

    private int isresource = 0;

    private int isprofile = 0;

    private int istype = 0;

    private int issysinfo = 0;

    private boolean isxml = false;

    private String lfn = null;

    private String pfn = null;

    private String profile = null;

    private String type = null;

    private String resource = null;

    private String system = null;

    private String file = null;

    private TransformationCatalog tc = null;

    private Map argsmap = null;

    private LogManager mLogger = LogManager.getInstance();

    private Version version = Version.instance();

    public TCClient() {
        super();
    }

    public void loadProperties() {
    }

    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[14 ];
        longopts[ 0 ] = new LongOpt( "add", LongOpt.NO_ARGUMENT, null, 'a' );
        longopts[ 1 ] = new LongOpt( "delete", LongOpt.NO_ARGUMENT, null, 'd' );
        longopts[ 2 ] = new LongOpt( "query", LongOpt.NO_ARGUMENT, null, 'q' );
        longopts[ 3 ] = new LongOpt( "lfn", LongOpt.REQUIRED_ARGUMENT, null,
            'l' );
        longopts[ 4 ] = new LongOpt( "pfn", LongOpt.REQUIRED_ARGUMENT, null,
            'p' );
        longopts[ 5 ] = new LongOpt( "profile", LongOpt.REQUIRED_ARGUMENT, null,
            'e' );
        longopts[ 6 ] = new LongOpt( "type", LongOpt.REQUIRED_ARGUMENT, null,
            't' );
        longopts[ 7 ] = new LongOpt( "file", LongOpt.REQUIRED_ARGUMENT, null,
            'f' );
        longopts[ 8 ] = new LongOpt( "help", LongOpt.NO_ARGUMENT, null, 'h' );
        longopts[ 9 ] = new LongOpt( "version", LongOpt.NO_ARGUMENT, null, 'V' );
        longopts[ 10 ] = new LongOpt( "verbose", LongOpt.NO_ARGUMENT, null, 'v' );
        longopts[ 11 ] = new LongOpt( "resource", LongOpt.REQUIRED_ARGUMENT, null,
            'r' );
        longopts[ 12 ] = new LongOpt( "system", LongOpt.REQUIRED_ARGUMENT, null,
            's' );
        longopts[ 13 ] = new LongOpt( "xml", LongOpt.NO_ARGUMENT, null,
            'x' );
        return longopts;
    }

    /**
     * Call the correct commands depending on options.
     * @param opts String[] The arguments obtained from the command line.
     */

    public void executeCommand( String[] opts ) {
        LongOpt[] longOptions = generateValidOptions();
        Getopt g = new Getopt( "TCClient", opts,
            "adqhvxVLPERTBSs:t:l:p:r:e:f:",
            longOptions, false );
        int option = 0;
        int noOfOptions = 0;
        int level = 0;
        while ( ( option = g.getopt() ) != -1 ) {
            switch ( option ) {
                case 'q': //output
                    query = 1;
                    break;
                case 'a':
                    add = 2;
                    break;
                case 'd':
                    delete = 4;
                    break;
                case 'B':
                    bulk = 1;
                    break;
                case 'L':
                    islfn = 2;
                    break;
                case 'P':
                    ispfn = 4;
                    break;
                case 'R':
                    isresource = 8;
                    break;
                case 'E':
                    isprofile = 16;
                    break;
                case 'T':
                    istype = 32;
                    break;
                case 'S':
                    issysinfo = 64;
                    break;
                case 't':
                    type = g.getOptarg();
                    break;
                case 's':
                    system = g.getOptarg();
                    break;
                case 'l':
                    lfn = g.getOptarg();
                    break;
                case 'p':
                    pfn = g.getOptarg();
                    break;
                case 'e':
                    if ( profile != null ) {
                        profile = profile + ";" + g.getOptarg();
                    } else {
                        profile = g.getOptarg();
                    }
                    break;
                case 'f':
                    file = g.getOptarg();
                    break;
                case 'r':
                    resource = g.getOptarg();
                    break;
                case 'h': //help
                    printLongVersion();
                    System.exit( 0 );
                    break;
                case 'V': //version
                    mLogger.log( version.toString(),
                        LogManager.INFO_MESSAGE_LEVEL );
                    System.exit( 0 );
                case 'v': //Verbose mode
                    level++;
                    break;
                case 'x': //Is XML
                    isxml = true;
                    break;
                default:
                    mLogger.log( "Unrecognized Option : " + ( char ) option,
                        LogManager.FATAL_MESSAGE_LEVEL );
                    printShortVersion();
                    System.exit( 1 );
                    break;
            }
        }
        mLogger.setLevel( level );

        //calculating the value of the trigger
        int trigger = bulk + islfn + ispfn + isresource + isprofile + istype +
            issysinfo;

        argsmap = new HashMap( 11 );
        argsmap.put( "trigger", new java.lang.Integer( trigger ) );
        argsmap.put( "lfn", lfn );
        argsmap.put( "pfn", pfn );
        argsmap.put( "profile", profile );
        argsmap.put( "type", type );
        argsmap.put( "resource", resource );
        argsmap.put( "system", system );
        argsmap.put( "file", file );
        argsmap.put( "isxml", new Boolean( isxml ) );

        //Select what operation is to be performed.
        int operationcase = query + add + delete;

        //load the transformation catalog if required
        try{
            if (operationcase == 1 || operationcase == 4 || operationcase == 2) {
                tc = TransformationFactory.loadInstance();
            }
        }
        catch ( FactoryException fe){
            mLogger.log( fe.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            System.exit( 2 );
        }

        switch ( operationcase ) {
            case 1: //QUERY OPERATION SELECTED
                TCQuery tcquery = new TCQuery( tc, mLogger, argsmap );
                tcquery.doQuery();

                break;
            case 2: //ADD OPERATION SELECTED
                TCAdd tcadd = new TCAdd( tc, mLogger, argsmap );
                tcadd.doAdds();
                break;

            case 4: //DELETE OPERATION SELECTED
                TCDelete tcdelete = new TCDelete( tc, mLogger, argsmap );
                tcdelete.doDelete();
                break;
            default: //ERROR IN SELECTION OPERATION
                mLogger.log(
                    "Please specify the correct operation for the client." +
                    "Only one operation can be done at a time.",
                    LogManager.FATAL_MESSAGE_LEVEL );
                this.printShortVersion();
                System.exit( -1 );
        }
    }

    public void printShortVersion() {
        String text =
            "\n " + version.toString() +
            "\n Usage :tc-client  [ operation ]  [ operation arguments ]" +
            "\n Type tc-client -h for more details";

        mLogger.log( text, LogManager.ERROR_MESSAGE_LEVEL );
        System.exit( 1 );
    }

    public void printLongVersion() {
        String text =
            "\n" + version.toString() +
            "\n" +
            "\n tc-client - This client is used to add, delete, query any Tranformation Catalog implemented to the TC interface." +
            "\n" +
            "\n Usage: tc-client  [Operation] [Triggers] [Options]...." +
            "\n" +
            "\n Operations :" +
            "\n ------------" +
            "\n  Always one of these operations have to be specified." +
            "\n" +
            "\n -a | --add     Perform addition operations on the TC." +
            "\n -d | --delete  Perform delete operations on the TC." +
            "\n -q | --query   Perform query operations on the TC." +
            "\n" +
            "\n Triggers :" +
            "\n ----------" +
            "\n" +
            "\n -L Triggers an operation on a logical transformation" +
            "\n -P Triggers an operation on a physical transformation" +
            "\n -R Triggers an operation on a resource" +
            "\n -E Triggers an operation on a Profile" +
            "\n -T Triggers an operation on a Type" +
            "\n -B Triggers a bulk operation." +
            "\n" +
            "\n Options :" +
            "\n ---------" +
            "\n" +
            "\n -l | --lfn  <logical txmation>  The logical transformation to be added in the format NAMESPACE::NAME:VERSION." +
            "\n                                 (The name is always required, namespace and version are optional.)" +
            "\n -p | ---pfn <physical txmation> The physical transfromation to be added. " +
            "\n                                 For INSTALLED executables its a local file path, for all others its a url." +
            "\n -t | --type <type of txmation>  The type of physical transformation. Valid values are :" +
            "\n                                 INSTALLED, STATIC_BINARY, DYNAMIC_BINARY, SCRIPT, SOURCE, PACMAN_PACKAGE. " +
            "\n -r | --resource <resource id>   The Id of the resource where the transformation is located. " +
            "\n -e | --profile <profiles>       The profiles belonging to the transformation." +
            "\n                                 Mulitple profiles of same namespace can be added simultaneously" +
            "\n                                 by seperating them with a comma \",\"." +
            "\n                                 Each profile section is written as NAMESPACE::KEY=VALUE,KEY2=VALUE2 " +
            "\n                                 e.g. ENV::JAVA_HOME=/usr/bin/java2,PEGASUS_HOME=/usr/local/vds" +
            "\n                                 To add muliple namespaces you need to repeat the -e option for each namespace." +
            "\n                                 e.g -e ENV::JAVA_HOME=/usr/bin/java -e GLOBUS::JobType=MPI,COUNT=10" +
            "\n -s | --system <system type>     The architecture,os and glibc if any for the executable." +
            "\n                                 Each system info is written in the form ARCH::OS:OSVER:GLIBC" +
            "\n                                 The allowed ARCH's are INTEL32, INTEL64, SPARCV7, SPARCV9" +
            "\n                                 The allowed OS's are LINUX, SUNOS, AIX" +
            "\n" +
            "\n Other Options :" +
            "\n ---------------" +
            "\n" +
            "\n --verbose | -v    increases the verbosity level" +
            "\n --version | -V    Displays the version number of the Griphyn Virtual Data System software " +
            "\n --help    | -h    Generates this help" +
            "\n" +
            "\n Valid Combinations :" +
            "\n --------------------" +
            "\n ADD" +
            "\n ---" +
            "\n " +
            "\n\tAdd TC Entry       : -a -l <lfn> -p <pfn> -r <resource> [-t <type>] [-s <system>] [-e <profiles> ....]" +
            "\n\tAdd PFN Profile    : -a -P -E -p <pfn> -t <type> -r <resource> -e <profiles> ...." +
            "\n\tAdd LFN Profile    : -a -L -E -l <lfn> -e <profiles> ...." +
            "\n\tAdd Bulk Entries   : -a -B -f <file>" +
            "\n" +
            "\n DELETE" +
            "\n ------" +
            "\n" +
            "\n\tDelete all TC      : -d -BPRELST " +
            "\n\t                    (!!!WARNING : THIS DELETETS THE ENTIRE TC!!!)" +
            "\n\tDelete by LFN      : -d -L -l <lfn> [-r <resource>] [-t <type>]" +
            "\n\tDelete by PFN      : -d -P -l <lfn> -p <pfn> [-r <resource>] [-t type]" +
            "\n\tDelete by Type     : -d -T -t <type> [-r <resource>]" +
            "\n\tDelete by Resource : -d -R -r <resource>" +
            "\n\tDelete by SysInfo  : -d -S -s <sysinfo>" +
            "\n\tDelete Pfn Profile : -d -P -E -p <pfn> -r <resource> -t <type> [-e <profiles> ....]" +
            "\n\tDelete Lfn Profile : -d -L -E -l <lfn> [-e <profiles> .....]" +
            "\n" +
            "\n QUERY" +
            "\n -----" +
            "\n " +
            "\n\tQuery Bulk         : -q -B" +
            "\n\tQuery LFN          : -q -L [-r <resource>] [-t <type>]" +
            "\n\tQuery PFN          : -q -P -l <lfn> [-r <resource>] [-t <type>]" +
            "\n\tQuery Resource     : -q -R [-l <lfn>] [-t <type>]" +
            "\n\tQuery Lfn Profile  : -q -L -E -l <lfn>" +
            "\n\tQuery Pfn Profile  : -q -P -E -p <pfn> -r <resource> -t <type>" +
            "\n";

        mLogger.log( text, LogManager.INFO_MESSAGE_LEVEL );
        System.exit( 0 );
    }

    public static void main( String[] args ) throws Exception {
        TCClient client = new TCClient();
        client.executeCommand( args );
    }

}
