/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.client;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationFactory;
import edu.isi.pegasus.planner.catalog.transformation.client.TCAdd;
import edu.isi.pegasus.planner.catalog.transformation.client.TCDelete;
import edu.isi.pegasus.planner.catalog.transformation.client.TCQuery;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.util.HashMap;
import java.util.Map;

/**
 * A common client to add, modify, delete, query any Transformation Catalog implementation.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TCClient extends Executable {

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

    private boolean isoldformat = false;

    private String lfn = null;

    private String pfn = null;

    private String profile = null;

    private String type = null;

    private String resource = null;

    private String system = null;

    private String file = null;

    private TransformationCatalog tc = null;

    private Map argsmap = null;

    private Version version = Version.instance();

    public TCClient() {
        super();
    }

    public void initialize(String[] opts) {
        super.initialize(opts);
    }

    public void loadProperties() {}

    /**
     * Sets up the logging options for this class. Looking at the properties file, sets up the
     * appropriate writers for output and stderr.
     */
    protected void setupLogging() {
        // setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance(mProps);
        mLogger.logEventStart(
                "event.pegasus.tc-client",
                "client.version",
                mVersion,
                LogManager.DEBUG_MESSAGE_LEVEL);
    }

    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[16];
        longopts[0] = new LongOpt("add", LongOpt.NO_ARGUMENT, null, 'a');
        longopts[1] = new LongOpt("delete", LongOpt.NO_ARGUMENT, null, 'd');
        longopts[2] = new LongOpt("query", LongOpt.NO_ARGUMENT, null, 'q');
        longopts[3] = new LongOpt("lfn", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        longopts[4] = new LongOpt("pfn", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        longopts[5] = new LongOpt("profile", LongOpt.REQUIRED_ARGUMENT, null, 'e');
        longopts[6] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[7] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        longopts[8] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[9] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        longopts[10] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        longopts[11] = new LongOpt("resource", LongOpt.REQUIRED_ARGUMENT, null, 'r');
        longopts[12] = new LongOpt("system", LongOpt.REQUIRED_ARGUMENT, null, 's');
        longopts[13] = new LongOpt("xml", LongOpt.NO_ARGUMENT, null, 'x');
        longopts[14] = new LongOpt("oldformat", LongOpt.NO_ARGUMENT, null, 'o');
        longopts[15] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        return longopts;
    }

    /**
     * Call the correct commands depending on options.
     *
     * @param opts String[] The arguments obtained from the command line.
     */
    public void executeCommand() {
        String[] opts = getCommandLineOptions();
        if (opts.length == 0) {
            mLogger.log("Please provide the required options.", LogManager.ERROR_MESSAGE_LEVEL);
            this.printShortVersion();
            System.exit(1);
        }
        LongOpt[] longOptions = generateValidOptions();
        Getopt g =
                new Getopt("TCClient", opts, "adqhvxoVLPERTBSs:t:l:p:r:e:f:c:", longOptions, false);
        int option = 0;
        int level = 0;
        while ((option = g.getopt()) != -1) {
            switch (option) {
                case 'q': // output
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
                    if (profile != null) {
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
                case 'h': // help
                    printLongVersion();
                    System.exit(0);
                    break;
                case 'V': // version
                    System.out.println(version.toString());
                    System.exit(0);
                case 'v': // Verbose mode
                    level++;
                    break;
                case 'x': // Is XML
                    isxml = true;
                    if (isoldformat) {
                        throw new IllegalArgumentException(
                                "Error: Illegal Argument passed. Options -x and -o cannot be set at the same time");
                    }
                    break;
                case 'o': // Is Old format
                    isoldformat = true;
                    if (isxml) {
                        throw new IllegalArgumentException(
                                "Error: Illegal Argument passed. Options -x and -o cannot be set at the same time");
                    }
                    break;
                case 'c':
                    // do nothing
                    break;
                default:
                    mLogger.log(
                            "Unrecognized option or Invalid argument to option "
                                    + (char) g.getOptopt(),
                            LogManager.FATAL_MESSAGE_LEVEL);
                    printShortVersion();
                    System.exit(1);
                    break;
            }
        }
        if (level > 0) {
            mLogger.setLevel(level);
        } else {
            mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        }

        // calculating the value of the trigger
        int trigger = bulk + islfn + ispfn + isresource + isprofile + istype + issysinfo;

        argsmap = new HashMap(11);
        argsmap.put("trigger", new java.lang.Integer(trigger));
        argsmap.put("lfn", lfn);
        argsmap.put("pfn", pfn);
        argsmap.put("profile", profile);
        argsmap.put("type", type);
        argsmap.put("resource", resource);
        argsmap.put("system", system);
        argsmap.put("file", file);
        argsmap.put("isxml", new Boolean(isxml));
        argsmap.put("isoldformat", new Boolean(isoldformat));

        // Select what operation is to be performed.
        int operationcase = query + add + delete;

        // load the transformation catalog if required
        try {
            if (operationcase == 1 || operationcase == 4 || operationcase == 2) {
                this.mProps.setProperty(TransformationCatalog.MODIFY_FOR_FILE_URLS_KEY, "false");
                tc = TransformationFactory.loadInstance(this.mProps);
            }
        } catch (FactoryException fe) {
            mLogger.log(convertException(fe, mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(2);
        }
        try {
            switch (operationcase) {
                case 1: // QUERY OPERATION SELECTED
                    TCQuery tcquery = new TCQuery(tc, mLogger, argsmap);
                    tcquery.doQuery();

                    break;
                case 2: // ADD OPERATION SELECTED
                    TCAdd tcadd = new TCAdd(tc, mLogger, argsmap);
                    tcadd.doAdds();
                    break;

                case 4: // DELETE OPERATION SELECTED
                    TCDelete tcdelete = new TCDelete(tc, mLogger, argsmap);
                    tcdelete.doDelete();
                    break;
                default: // ERROR IN SELECTION OPERATION
                    mLogger.log(
                            "Please specify the correct operation for the client."
                                    + "Only one operation can be done at a time.",
                            LogManager.FATAL_MESSAGE_LEVEL);
                    this.printShortVersion();
                    System.exit(-1);
            }
        } finally {
            if (tc != null) {
                if (!tc.isClosed()) {
                    tc.close();
                }
            }
        }
        mLogger.logEventCompletion(LogManager.DEBUG_MESSAGE_LEVEL);
    }

    public void printShortVersion() {
        String text =
                "\n "
                        + version.toString()
                        + "\n Usage :pegasus-tc-client  [ operation ]  [ operation arguments ]"
                        + "\n Type pegasus-tc-client -h for more details";

        System.out.println(text);
        System.exit(1);
    }

    public void printLongVersion() {
        String text =
                "\n"
                        + version.toString()
                        + "\n"
                        + "\n pegasus-tc-client - This client is used to add, delete, query any Tranformation Catalog implemented to the TC interface."
                        + "\n"
                        + "\n Usage: pegasus-tc-client  [Operation] [Triggers] [Options]...."
                        + "\n"
                        + "\n Operations :"
                        + "\n ------------"
                        + "\n  Always one of these operations have to be specified."
                        + "\n"
                        + "\n -a | --add     Perform addition operations on the TC."
                        + "\n -d | --delete  Perform delete operations on the TC."
                        + "\n -q | --query   Perform query operations on the TC."
                        + "\n"
                        + "\n Triggers :"
                        + "\n ----------"
                        + "\n"
                        + "\n -L Triggers an operation on a logical transformation"
                        + "\n -P Triggers an operation on a physical transformation"
                        + "\n -R Triggers an operation on a resource"
                        + "\n -E Triggers an operation on a Profile"
                        + "\n -T Triggers an operation on a Type"
                        + "\n -B Triggers a bulk operation."
                        + "\n"
                        + "\n Options :"
                        + "\n ---------"
                        + "\n"
                        + "\n -l | --lfn  <logical txmation>  The logical transformation to be added in the format NAMESPACE::NAME:VERSION."
                        + "\n                                 (The name is always required, namespace and version are optional.)"
                        + "\n -p | ---pfn <physical txmation> The physical transfromation to be added. "
                        + "\n                                 For INSTALLED executables its a local file path, for all others its a url."
                        + "\n -t | --type <type of txmation>  The type of physical transformation. Valid values are :"
                        + "\n                                 INSTALLED, STAGEABLE. "
                        + "\n -r | --resource <resource id>   The Id of the resource where the transformation is located. "
                        + "\n -e | --profile <profiles>       The profiles belonging to the transformation."
                        + "\n                                 Mulitple profiles of same namespace can be added simultaneously"
                        + "\n                                 by seperating them with a comma \",\"."
                        + "\n                                 Each profile section is written as NAMESPACE::KEY=VALUE,KEY2=VALUE2 "
                        + "\n                                 e.g. ENV::JAVA_HOME=/usr/bin/java2,PEGASUS_HOME=/usr/local/vds"
                        + "\n                                 To add muliple namespaces you need to repeat the -e option for each namespace."
                        + "\n                                 e.g -e ENV::JAVA_HOME=/usr/bin/java -e GLOBUS::JobType=MPI,COUNT=10"
                        + "\n -s | --system <system type>     The architecture,os and glibc if any for the executable."
                        + "\n                                 Each system info is written in the form ARCH::OS:OSVER:GLIBC"
                        + "\n                                 The allowed ARCH's are x86, x86_64, ppc, ppc_64, ia64,  sparcv7, sparcv9, amd64"
                        + "\n                                 The allowed OS's are LINUX, SUNOS, AIX, MACOSX, WINDOWS"
                        + "\n"
                        + "\n Other Options :"
                        + "\n ---------------"
                        + "\n"
                        + "\n --xml       | -x  Generates the output in the xml format "
                        + "\n --oldformat | -o  Generates the output in the old single line format "
                        + "\n --conf      | -c  path to  property file"
                        + "\n --verbose   | -v  increases the verbosity level"
                        + "\n --version   | -V  Displays the version number of the Griphyn Virtual Data System software "
                        + "\n --help      | -h  Generates this help"
                        + "\n"
                        + "\n Valid Combinations :"
                        + "\n --------------------"
                        + "\n ADD"
                        + "\n ---"
                        + "\n "
                        + "\n\tAdd TC Entry       : -a -l <lfn> -p <pfn> -r <resource> [-t <type>] [-s <system>] [-e <profiles> ....]"
                        + "\n\tAdd PFN Profile    : -a -P -E -p <pfn> -t <type> -r <resource> -e <profiles> ...."
                        + "\n\tAdd LFN Profile    : -a -L -E -l <lfn> -e <profiles> ...."
                        + "\n\tAdd Bulk Entries   : -a -B -f <file>"
                        + "\n"
                        + "\n DELETE"
                        + "\n ------"
                        + "\n"
                        + "\n\tDelete all TC      : -d -BPRELST "
                        + "\n\t                    (!!!WARNING : THIS DELETES THE ENTIRE TC!!!)"
                        + "\n\tDelete by LFN      : -d -L -l <lfn> [-r <resource>] [-t <type>]"
                        + "\n\tDelete by PFN      : -d -P -l <lfn> -p <pfn> [-r <resource>] [-t type]"
                        + "\n\tDelete by Type     : -d -T -t <type> [-r <resource>]"
                        + "\n\tDelete by Resource : -d -R -r <resource>"
                        + "\n\tDelete by SysInfo  : -d -S -s <sysinfo>"
                        + "\n\tDelete Pfn Profile : -d -P -E -p <pfn> -r <resource> -t <type> [-e <profiles> ....]"
                        + "\n\tDelete Lfn Profile : -d -L -E -l <lfn> [-e <profiles> .....]"
                        + "\n"
                        + "\n QUERY"
                        + "\n -----"
                        + "\n "
                        + "\n\tQuery Bulk         : -q -B"
                        + "\n\tQuery LFN          : -q -L [-r <resource>] [-t <type>]"
                        + "\n\tQuery PFN          : -q -P -l <lfn> [-r <resource>] [-t <type>]"
                        + "\n\tQuery Resource     : -q -R -l <lfn> [-t <type>]"
                        + "\n\tQuery Lfn Profile  : -q -L -E -l <lfn>"
                        + "\n\tQuery Pfn Profile  : -q -P -E -p <pfn> -r <resource> -t <type>"
                        + "\n";

        System.out.println(text);
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        TCClient client = new TCClient();
        client.initialize(args);
        client.executeCommand();
    }
}
