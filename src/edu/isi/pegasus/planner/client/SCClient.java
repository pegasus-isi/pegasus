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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteDataVisitor;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.XML3PrintVisitor;
import edu.isi.pegasus.planner.catalog.site.classes.XML4PrintVisitor;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

/**
 * A client to convert site catalog between different formats.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
public class SCClient extends Executable {

    /** The input files. */
    private List<String> mInputFiles;

    /** The output file that is written out. */
    private String mOutputFile;

    /** The output format for the site catalog. */
    private String mOutputFormat;

    /** The input format for the site catalog. */
    private String mInputFormat;

    /** Denotes the logging level that is to be used for logging the messages. */
    private int mLoggingLevel;
    
    /**
     * Boolean indicating whether to do variable expansion or not
     */
    private boolean mDoVariableExpansion;

    /** The default constructor. */
    public SCClient() {
        super();
    }

    public void initialize(String[] opts) {
        super.initialize(opts);
        // the output format is whatever user specified in the properties
        mOutputFormat = "YAML";
        mInputFormat = "XML";
        mLoggingLevel = LogManager.WARNING_MESSAGE_LEVEL;
        // mText = false;
        //PM-1464 by default variable expansion is disabled.
        mDoVariableExpansion = false;
        mInputFiles = null;
        mOutputFile = null;
    }

    /**
     * Sets up the logging options for this class. Looking at the properties file, sets up the
     * appropriate writers for output and stderr.
     */
    protected void setupLogging() {
        // setup the logger for the default streams.
        mLogger = LogManagerFactory.loadSingletonInstance(mProps);
        mLogger.logEventStart("event.pegasus.pegasus-sc-converter", "pegasus.version", mVersion);
    }
    /** Loads all the properties that would be needed by the Toolkit classes */
    public void loadProperties() {}

    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[9];
        longopts[0] = new LongOpt("input", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        longopts[1] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[2] = new LongOpt("oformat", LongOpt.REQUIRED_ARGUMENT, null, 'O');
        longopts[3] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[4] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        longopts[5] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        longopts[6] = new LongOpt("quiet", LongOpt.NO_ARGUMENT, null, 'q');
        longopts[7] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        longopts[8] = new LongOpt("expand", LongOpt.NO_ARGUMENT, null, 'e');
        return longopts;
    }

    /**
     * Call the correct commands depending on options.
     *
     * @param opts Command options
     */
    public void executeCommand() throws IOException {
        LongOpt[] longOptions = generateValidOptions();

        Getopt g =
                new Getopt(
                        "SCClient", getCommandLineOptions(), "elhvqVi:o:O:c:", longOptions, false);

        int option = 0;
        while ((option = g.getopt()) != -1) {
            switch (option) {
                case 'e': // expand
                    mDoVariableExpansion = true;
                    break;
                
                case 'i': // input
                    StringTokenizer str = new StringTokenizer(g.getOptarg(), ",");
                    mInputFiles = new ArrayList<String>(str.countTokens());
                    while (str.hasMoreTokens()) {
                        mInputFiles.add(str.nextToken());
                    }
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

                    /*
                    case 'l': // Precedence for local or remote
                        mLocalPrec = true;
                        break;
                         */

                case 'v': // Verbose mode
                    incrementLogging();
                    break;

                case 'q': // Quiet mode
                    decrementLogging();
                    break;

                case 'c': // conf
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
        if (mInputFiles == null
                || mInputFiles.isEmpty()
                || mOutputFile == null
                || mOutputFile.isEmpty()) {
            mLogger.log(
                    "Please provide the input and the output file", LogManager.ERROR_MESSAGE_LEVEL);
            this.printShortVersion();
            System.exit(1);
        }
        String result = this.parseInputFiles(mInputFiles, mInputFormat, mOutputFormat);
        if (result != null) {
            // write out the result to the output file
            this.toFile(mOutputFile, result);
        }
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
     * Parses the input files in the input format and returns a String in the output format.
     *
     * @param inputFiles list of input files that need to be converted
     * @param inputFormat input format of the input files
     * @param outputFormat output format of the output file
     * @return String in output format
     * @throws java.io.IOException
     */
    public String parseInputFiles(List<String> inputFiles, String inputFormat, String outputFormat)
            throws IOException {
        // sanity check
        if (inputFiles == null || inputFiles.isEmpty()) {
            throw new IOException("Input files not specified. Specify the --input option");
        }

        // mLogger.log( "Input  format detected is " + inputFormat , LogManager.DEBUG_MESSAGE_LEVEL
        // );
        mLogger.log("Output format detected is " + outputFormat, LogManager.DEBUG_MESSAGE_LEVEL);

        SiteStore result = new SiteStore();
        for (String inputFile : inputFiles) {
            // switch on input format.
            if (inputFormat.equals("XML")) {
                SiteCatalog catalog = null;

                /* load the catalog using the factory */
                try {
                    mProps.setProperty("pegasus.catalog.site.file", inputFile);
                    mProps.setProperty(SiteCatalog.c_prefix, mInputFormat);
                    mProps.setProperty(SiteCatalog.VARIABLE_EXPANSION_KEY, Boolean.toString(mDoVariableExpansion));
                    catalog = SiteFactory.loadInstance(mProps);

                    /* load all sites in site catalog */
                    List<String> s = new ArrayList<String>(1);
                    s.add("*");
                    mLogger.log(
                            "Loaded  " + catalog.load(s) + " number of sites ",
                            LogManager.DEBUG_MESSAGE_LEVEL);

                    /* query for the sites, and print them out */
                    mLogger.log(
                            "Sites loaded are " + catalog.list(), LogManager.DEBUG_MESSAGE_LEVEL);
                    for (String site : catalog.list()) {
                        result.addEntry(catalog.lookup(site));
                    }
                    if (outputFormat.contains("XML")) {
                        SiteDataVisitor xml = null;
                        StringWriter writer = new StringWriter();

                        if (outputFormat.equals("XML3")) {
                            xml = new XML3PrintVisitor();
                        } else {
                            xml = new XML4PrintVisitor();
                        }

                        xml.initialize(writer);
                        result.accept(xml);

                        return writer.toString();
                    } else {
                        if (outputFormat.equals("YAML")) {
                            // in case of yaml we write it directly to the output file so we are
                            // returning null..
                            ObjectMapper mapper =
                                    new ObjectMapper(
                                            new YAMLFactory()
                                                    .configure(
                                                            YAMLGenerator.Feature.INDENT_ARRAYS,
                                                            true));
                            mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                            return mapper.writeValueAsString(result);
                        }
                    }
                } finally {
                    /* close the connection */
                    try {
                        catalog.close();
                    } catch (Exception e) {
                    }
                }
            } else {
                throw new IOException(
                        "Invalid input format. Only input format supported in XML. The client will auto-detect if the input is in version 3 or 4.");
            } // end of input format xml
        } // end of iteration through input files.

        return null;
    }

    /** Returns the short help. */
    public void printShortVersion() {
        String text =
                "\n $Id$ "
                        + "\n "
                        + getGVDSVersion()
                        + "\n Usage: pegasus-sc-converter [-Dprop  [..]]  -i <list of input files> -o <output file to write> "
                        + "\n        [-O <output format>] [-c <path to property file>] [-v] [-q] [-V] [-h]\n";

        System.out.print(text);
    }

    public void printLongVersion() {
        String text =
                "\n $Id$ "
                        + "\n "
                        + getGVDSVersion()
                        + "\n pegasus-sc-converter - Parses the site catalogs in old format (XML and generates site catalog in " 
                        + "\n new format (YAML)"
                        + "\n "
                        + "\n Usage: pegasus-sc-converter [-Dprop  [..]]  --input <list of input files> --output <output file to write> "
                        + "\n          [--iformat input format] [--oformat <output format>] [--conf <path to property file>] [--verbose]"
                        + "\n          [--quiet] [--Version] [--help]"
                        + "\n"
                        + "\n"
                        + "\n Mandatory Options "
                        + "\n"
                        + "\n -i |--input      comma separated list of input files to convert "
                        + "\n -o |--output     the output file to which the output needs to be written to."
                        + "\n"
                        + "\n"
                        + "\n Other Options "
                        + "\n -O |--oformat    the output format of the file. Usually [YAML] "
                        + "\n -c |--conf       path to  property file"
                        + "\n -e |--expand     sets variable expansion on. Any variables in input files " 
                        + "\n                  will be expanded and their values will be written out to " 
                        + "\n                  output site catalog"
                        + "\n -v |--verbose    increases the verbosity of messages about what is going on"
                        + "\n -q |--quiet      decreases the verbosity of messages about what is going on"
                        + "\n -V |--version    displays the version of the Pegasus Workflow Planner"
                        + "\n -h |--help       generates this help."
                        + "\n"
                        + "\n"
                        + "\n Example Usage "
                        + "\n"
                        + "\n pegasus-sc-converter  -i sites.xml -o sites.yml  -O YAML -vvvvv\n";

        System.out.print(text);
    }

    /**
     * Writes out to a file, a string.
     *
     * @param filename the fully qualified path name to the file.
     * @param output the text that needs to be written to the file.
     * @throws IOException
     */
    public void toFile(String filename, String output) throws IOException {
        if (filename == null) {
            throw new IOException(
                    "Please specify a file to write the output to using --output option ");
        }

        File outfile = new File(filename);

        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(outfile)));
        pw.println(output);
        pw.close();
        mLogger.log(
                "Written out the converted file to " + filename, LogManager.CONSOLE_MESSAGE_LEVEL);
    }

    public static void main(String[] args) throws Exception {

        SCClient me = new SCClient();
        int result = 0;
        double starttime = new Date().getTime();
        double execTime = -1;

        try {
            me.initialize(args);
            me.executeCommand();
        } catch (IOException ioe) {
            me.log(convertException(ioe, me.getLoggingLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 1;
        } catch (FactoryException fe) {
            me.log(convertException(fe, me.getLoggingLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        } catch (Exception e) {
            // unaccounted for exceptions
            me.log(convertException(e, me.getLoggingLevel()), LogManager.FATAL_MESSAGE_LEVEL);
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
