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
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Metadata;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * A client to convert replica catalog between different formats.
 *
 * @author Prasanth Thomas
 * @version $Revision$
 */
public class RCConverter extends Executable {

    /** The textual format. */
    private static String FILE_FORMAT = "File";

    /** YAML Format */
    private static String YAML_FORMAT = "YAML";

    /** REGEX Format */
    private static String REGEX_FORMAT = "Regex";

    /** The supported replica formats. */
    private static final String[] SUPPORTED_REPLICA_FORMATS = {
        FILE_FORMAT, REGEX_FORMAT, YAML_FORMAT
    };

    /** The input files. */
    private List<String> mInputFiles;

    /** The output file that is written out. */
    private String mOutputFile;

    /** The output format for the replica catalog. */
    private String mOutputFormat;

    /** The input format for the replica catalog. */
    private String mInputFormat;

    /** Denotes the logging level that is to be used for logging the messages. */
    private int mLoggingLevel;

    /** Boolean indicating whether to do variable expansion or not */
    private boolean mDoVariableExpansion;

    /** The default constructor. */
    public RCConverter() {
        super();
    }

    protected void initialize(String[] opts) {
        super.initialize(opts);
        // the output format is whatever user specified in the properties
        mOutputFormat = RCConverter.YAML_FORMAT;
        mInputFormat = RCConverter.FILE_FORMAT;
        mInputFiles = null;
        mOutputFile = null;
        mLoggingLevel = LogManager.WARNING_MESSAGE_LEVEL;
        // PM-1491 by default have variable expansion disabled
        mDoVariableExpansion = false;
    }

    /**
     * Sets up the logging options for this class. Looking at the properties file, sets up the
     * appropriate writers for output and stderr.
     */
    protected void setupLogging() {
        // setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance(mProps);
        mLogger.logEventStart("event.pegasus.pegasus-rc-converter", "pegasus.version", mVersion);
    }
    /** Loads all the properties that would be needed by the Toolkit classes */
    public void loadProperties() {}

    /**
     * Generates the list of valid options for the tc-converter client
     *
     * @return LongOpt[] list of valid options
     */
    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[10];
        longopts[0] = new LongOpt("input", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        longopts[1] = new LongOpt("iformat", LongOpt.REQUIRED_ARGUMENT, null, 'I');
        longopts[2] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[3] = new LongOpt("oformat", LongOpt.REQUIRED_ARGUMENT, null, 'O');
        longopts[4] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[5] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        longopts[6] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        longopts[7] = new LongOpt("quiet", LongOpt.NO_ARGUMENT, null, 'q');
        longopts[8] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[9] = new LongOpt("expand", LongOpt.NO_ARGUMENT, null, 'e');

        return longopts;
    }

    /**
     * Call the correct commands depending on options.
     *
     * @param opts Command options
     */
    public void executeCommand() throws IOException {
        String[] opts = getCommandLineOptions();
        if (opts.length == 0) {
            mLogger.log("Please provide the required options.", LogManager.ERROR_MESSAGE_LEVEL);
            this.printShortVersion();
            System.exit(1);
        }

        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt("TCConverter", opts, "ehVvqI:i:O:o:c:", longOptions, false);

        int option = 0;
        int noOfOptions = 0;
        while ((option = g.getopt()) != -1) {
            switch (option) {
                case 'e': // expand
                    mDoVariableExpansion = true;
                    break;

                case 'i': // input
                    StringTokenizer str = new StringTokenizer(g.getOptarg(), ",");
                    mInputFiles = new ArrayList(str.countTokens());
                    while (str.hasMoreTokens()) {
                        mInputFiles.add(str.nextToken());
                    }
                    break;

                case 'I': // iformat
                    mInputFormat = g.getOptarg();
                    break;

                case 'o': // output
                    mOutputFile = g.getOptarg();
                    break;

                case 'O': // oformat
                    mOutputFormat = g.getOptarg();
                    break;

                case 'h': // help
                    printLongVersion();
                    System.exit(0);
                    break;

                case 'V': // version
                    System.out.println(getGVDSVersion());
                    System.exit(0);
                    break;

                case 'v': // Verbose mode
                    incrementLogging();
                    break;

                case 'q': // Quiet mode
                    decrementLogging();
                    break;

                case 'c':
                    // do nothing
                    break;

                default:
                    mLogger.log(
                            "Unrecognized option or Invalid argument to option : "
                                    + (char) g.getOptopt(),
                            LogManager.FATAL_MESSAGE_LEVEL);
                    printShortVersion();
                    System.exit(1);
            }
        }
        if (getLoggingLevel() >= 0) {
            // set the logging level only if -v was specified
            // else bank upon the the default logging level
            mLogger.setLevel(getLoggingLevel());
        } else {
            // set log level to FATAL only
            mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        }

        convertReplicaCatalogs();
    }

    /** Increments the logging level by 1. */
    public void incrementLogging() {
        mLoggingLevel++;
    }

    /** Decrements the logging level by 1. */
    public void decrementLogging() {
        mLoggingLevel--;
    }

    /**
     * Returns the logging level.
     *
     * @return the logging level.
     */
    public int getLoggingLevel() {
        return mLoggingLevel;
    }

    /**
     * Converts replica catalog from one format to another
     *
     * @throws IOException
     */
    private void convertReplicaCatalogs() throws IOException {

        mLogger.log("Input  format detected is " + mInputFormat, LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("Output format detected is " + mOutputFormat, LogManager.DEBUG_MESSAGE_LEVEL);
        // check if format is supported

        if (!isSupportedFormat(mInputFormat)) {
            StringBuffer error = new StringBuffer();
            error.append("Format not supported ! The supported input formats are [");
            for (String format : SUPPORTED_REPLICA_FORMATS) {
                error.append(format).append(" ");
            }
            error.append("]");
            throw new RuntimeException(error.toString());
        }

        if (!isSupportedFormat(mOutputFormat)) {
            StringBuffer error = new StringBuffer();
            error.append("Format not supported ! The supported output formats are [");
            for (String format : SUPPORTED_REPLICA_FORMATS) {
                error.append(format).append(" ");
            }
            error.append("]");
            throw new RuntimeException(error.toString());
        }
        if (new File(mOutputFile).exists()) {
            throw new IOException(
                    "The output file specified already exists. Please delete file " + mOutputFile);
        }
        ReplicaStore result = this.loadFrom(mInputFiles, mInputFormat);
        // write out the result to the output file
        this.convert(result, mOutputFormat, mOutputFile);
    }

    /**
     * Parses the input files in the input format and returns the output as a List of
     * ReplicaCatalogEntry objects
     *
     * @param inputFiles list of input files that need to be converted
     * @param inputFormat input format of the input files
     * @return List<ReplicaCatalogEntry> , null if no replica catalog entry exists.
     * @throws java.io.IOException
     */
    private ReplicaStore loadFrom(List<String> inputFiles, String inputFormat) throws IOException {
        // sanity check
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IOException("Input files not specified. Specify the --input option");
        }

        ReplicaStore result = new ReplicaStore();
        PegasusProperties props = (PegasusProperties) mProps.clone();
        props.setProperty(ReplicaCatalog.c_prefix, inputFormat);

        // always set readonly property to true for input catalogs
        String key = ReplicaCatalog.c_prefix + "." + ReplicaCatalog.READ_ONLY_KEY;
        props.setProperty(key, "true");

        // Sanity check
        for (String inputFile : inputFiles) {
            File input = new File(inputFile);
            if (!input.canRead()) {
                throw new IOException(
                        "File not found or cannot be read. " + input.getAbsolutePath());
            }
        }
        for (String inputFile : inputFiles) {
            props.setProperty("pegasus.catalog.replica.file", inputFile);
            ReplicaStore store = parseRC(props);
            for (Iterator<ReplicaLocation> it = store.replicaLocationIterator(); it.hasNext(); ) {
                result.add(it.next());
            }
        } // end of iteration through input files.

        return result;
    }

    /**
     * Parses the input format specified in the properties file and returns list of
     * ReplicaCatalogEntry
     *
     * @param props input format specified in the properties file
     * @return Replica Store
     */
    private ReplicaStore parseRC(PegasusProperties props) {
        // switch on input format.
        ReplicaCatalog catalog = null;
        ReplicaStore result = new ReplicaStore();
        try {
            PegasusBag bag = new PegasusBag();
            bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
            bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
            props.setProperty(
                    ReplicaCatalog.c_prefix + '.' + ReplicaCatalog.VARIABLE_EXPANSION_KEY,
                    Boolean.toString(mDoVariableExpansion));
            /* loadFrom the catalog using the factory */
            catalog = ReplicaFactory.loadInstance(bag);

            /* loadFrom all sites in replica catalog */
            Map<String, Collection<ReplicaCatalogEntry>> entries = catalog.lookup(new HashMap());
            int size = 0;
            for (Map.Entry<String, Collection<ReplicaCatalogEntry>> entry : entries.entrySet()) {
                size += entry.getValue().size();
            }
            mLogger.log("Loaded  " + size + " number of replicas ", LogManager.DEBUG_MESSAGE_LEVEL);
            result.add(entries);

        } catch (ReplicaCatalogException ife) {
            throw ife;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse replica catalog - " + e.getMessage());

        } finally {
            /* close the connection */
            if (catalog != null) {
                catalog.close();
            }
        }
        return result;
    }

    /**
     * Checks if it is a supported replica catalog format
     *
     * @param format the format
     * @return true , if format is supported, false otherwise.
     */
    private boolean isSupportedFormat(String format) {
        for (String sformat : SUPPORTED_REPLICA_FORMATS) {
            if (sformat.equals(format)) return true;
        }
        return false;
    }

    /** Prints the short help. */
    public void printShortVersion() {
        String text =
                "\n $Id$ "
                        + "\n "
                        + getGVDSVersion()
                        + "\n Usage: pegasus-rc-converter [-Dprop  [..]]  -I <input format> -O <output format> "
                        + "\n        [-i <list of input files>] [-o <output file to write>] "
                        +
                        /* Disable Database conversion options
                        "\n        [-N <database user name>] [-P <database user password>] [-U <database url>] [-H <database host>] " +
                         */
                        "\n        [-c <path to property file>] [-v] [-q] [-V] [-h] \n Type 'pegasus-rc-converter --help' for more help.";

        System.out.println(text);
    }

    public void printLongVersion() {
        StringBuffer text = new StringBuffer();
        text.append("\n $Id$ ");
        text.append("\n " + getGVDSVersion());
        text.append(
                "\n pegasus-rc-converter - Parses the replica catalogs in given input format ( File ,File ,Database ) and generates replica catalog into given output format ( File ,File ,Database )");
        text.append("\n ");
        text.append(
                "\n Usage: pegasus-rc-converter [-Dprop  [..]]  [--iformat <input format>] [--oformat <output format>]");
        text.append("\n       [--input <list of input files>] [--output <output file to write>] ");

        text.append(
                "\n       [--conf <path to property file>] [--verbose] [--quiet][--Version] [--help]");
        text.append("\n");
        text.append("\n");
        text.append("\n Mandatory Options ");
        text.append("\n");
        text.append(
                "\n -I |--iformat        the input format for the files . Can be [File, YAML] ");
        text.append("\n -O |--oformat        the output format of the file. Can be [File, YAML] ");
        text.append(
                "\n -i |--input          comma separated list of input files to convert.This option is mandatory when input format is File or file ");
        text.append(
                "\n -o |--output         the output file to which the output needs to be written to. This option is mandatory when output format is File or file ");
        text.append("\n");
        text.append("\n");
        text.append("\n Other Options ");
        text.append("\n");
        text.append("\n -c |--conf           path to  property file");
        text.append(
                "\n -e |--expand         sets variable expansion on. Any variables in input files "
                        + "\n                      will be expanded and their values will be written out to "
                        + "\n                      output replica catalog. ");
        text.append(
                "\n -v |--verbose        increases the verbosity of messages about what is going on");
        text.append(
                "\n -q |--quiet          decreases the verbosity of messages about what is going on");
        text.append("\n -V |--version        displays the version of the Pegasus Workflow Planner");
        text.append("\n -h |--help           generates this help.");
        text.append("\n");
        text.append("\n");
        text.append("\n Example Usage ");
        text.append("\n File to file format conversion :- ");
        text.append("  pegasus-rc-converter  -i cc.txt -I File -o rc.yml -v");

        System.out.println(text.toString());
    }

    /**
     * Converts Replica store to the given output format.
     *
     * @param output
     * @param format
     * @param filename
     * @throws IOException
     */
    private void convert(ReplicaStore output, String format, String filename) throws IOException {
        ReplicaCatalog catalog = null;
        PegasusProperties props = (PegasusProperties) mProps.clone();
        if (format.equals(FILE_FORMAT) || format.equals(YAML_FORMAT)) {

            if (filename == null) {
                throw new IOException(
                        "Please specify a file to write the output to using --output option ");
            }
            props.setProperty("pegasus.catalog.replica.file", filename);
        }

        props.setProperty("pegasus.catalog.replica", format);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        try {
            catalog = ReplicaFactory.loadInstance(bag);
        } catch (Exception e) {
            throw new RuntimeException("Exception encountered while loading catalog ", e);
        }

        // insert all the entries into the replica catalog
        for (Iterator<ReplicaLocation> it = output.replicaLocationIterator(); it.hasNext(); ) {
            ReplicaLocation rl = it.next();
            String lfn = rl.getLFN();
            Metadata m = rl.getAllMetadata();
            int insert = 0;
            for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                // PM-1534 and PM-1523 add metadata at LFN level
                // in the Replica Location object to individual RCE's
                for (Iterator<String> attribIT = m.getProfileKeyIterator(); attribIT.hasNext(); ) {
                    String key = attribIT.next();
                    rce.addAttribute(key, m.get(key));
                }

                try {
                    insert += catalog.insert(lfn, rce);
                } catch (Exception e) {
                    mLogger.log(
                            "Replica failed to add " + rce.toString(),
                            LogManager.ERROR_MESSAGE_LEVEL);
                }
            }

            mLogger.log(
                    "Inserted " + insert + " entries into output replica catalog ",
                    LogManager.INFO_MESSAGE_LEVEL);
        }

        // close the connection to the catalog
        catalog.close();

        mLogger.log(
                "Successfully converted Replica Catalog from "
                        + mInputFormat
                        + " to "
                        + mOutputFormat,
                LogManager.CONSOLE_MESSAGE_LEVEL);
        if (filename != null) {
            mLogger.log(
                    "The output replica catalog is in file  "
                            + new java.io.File(filename).getAbsolutePath(),
                    LogManager.CONSOLE_MESSAGE_LEVEL);
        }
    }

    /**
     * The main function
     *
     * @param args arguments passed at runtime
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {

        RCConverter me = new RCConverter();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime = -1;

        try {
            me.initialize(args);
            me.executeCommand();
        } catch (IOException ioe) {
            me.log(convertException(ioe, me.mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 1;
        } catch (FactoryException fe) {
            me.log(convertException(fe, me.mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        } catch (Exception e) {
            // unaccounted for exceptions
            me.log(convertException(e, me.mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 3;
        } finally {
            double endtime = new Date().getTime();
            execTime = (endtime - starttime) / 1000;
        }

        // warn about non zero exit code
        if (result != 0) {
            me.log("Non-zero exit-code " + result, LogManager.WARNING_MESSAGE_LEVEL);
        } else {
            // log the time taken to execute
            me.log(
                    "Time taken to execute is " + execTime + " seconds",
                    LogManager.INFO_MESSAGE_LEVEL);
        }

        me.log("Exiting with exitcode " + result, LogManager.DEBUG_MESSAGE_LEVEL);
        me.mLogger.logEventCompletion();
        System.exit(result);
    }
}
