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
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.XMLParser;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAX2Graph;
import edu.isi.pegasus.planner.parser.dax.DAX2LabelGraph;
import edu.isi.pegasus.planner.partitioner.Partitioner;
import edu.isi.pegasus.planner.partitioner.PartitionerFactory;
import edu.isi.pegasus.planner.partitioner.WriterCallback;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.File;
import java.util.Date;
import java.util.Map;

/**
 * The class ends up partitioning the dax into smaller daxes according to the various
 * algorithms/criteria, to be used for deferred planning.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PartitionDAX extends Executable {

    /** The name of the default partitioner that is loaded, if none is specified by the user. */
    public static final String DEFAULT_PARTITIONER_TYPE =
            PartitionerFactory.DEFAULT_PARTITIONING_CLASS;

    /** The path to the dax file that is to be partitioned. */
    private String mDAXFile;

    /** The directory in which the partition daxes are generated. */
    private String mDirectory;

    /**
     * The type of the partitioner to be used. Is the same as the name of the implementing class.
     */
    private String mType;

    /** The default constructor. */
    public PartitionDAX() {}

    /**
     * Initialize the PartitionDax object
     *
     * @param opts the command line argument passed to the PartitionDax
     */
    public void initalize(String[] opts) {
        super.initialize(opts);
        mDAXFile = null;
        mDirectory = ".";
        mType = DEFAULT_PARTITIONER_TYPE;
    }

    /**
     * The main function of the class, that is invoked by the jvm. It calls the executeCommand
     * function.
     *
     * @param args array of arguments.
     */
    public static void main(String[] args) {
        PartitionDAX pdax = new PartitionDAX();
        pdax.initalize(args);
        pdax.executeCommand();
    }

    /**
     * Executes the partition dax on the basis of the options given by the user.
     *
     * @param args the arguments array populated by the user options.
     */
    public void executeCommand() {
        int option = 0;
        LongOpt[] longOptions = generateValidOptions();
        Getopt g =
                new Getopt(
                        "PartitionDAX", getCommandLineOptions(), "vhVD:d:t:c:", longOptions, false);
        boolean help = false;
        boolean version = false;
        int status = 0;

        // log the starting time
        double starttime = new Date().getTime();
        int level = 0;
        while ((option = g.getopt()) != -1) {
            // System.out.println("Option tag " + option);
            switch (option) {
                case 'd': // dax
                    mDAXFile = g.getOptarg();
                    break;

                case 'D': // dir
                    mDirectory = g.getOptarg();
                    break;

                case 't': // type
                    mType = g.getOptarg();
                    break;

                case 'c': // conf
                    // do nothing
                    break;

                case 'v': // verbose
                    // set the verbose level in the logger
                    level++;
                    break;

                case 'V': // version
                    version = true;
                    break;

                case 'h': // help
                    help = true;
                    break;

                default: // same as help
                    mLogger.log(
                            "Incorrect option or option usage " + (char) g.getOptopt(),
                            LogManager.FATAL_MESSAGE_LEVEL);
                    printShortVersion();
                    System.exit(1);
                    break;
            }
        }
        if (level > 0) {
            // set the logging level only if -v was specified
            // else bank upon the the default logging level
            mLogger.setLevel(level);
        } else {
            // default level is warning
            mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        }

        if ((help && version) || help) {
            printLongVersion();
            System.exit(status);
        } else if (version) {
            // print the version message
            mLogger.log(getGVDSVersion(), LogManager.INFO_MESSAGE_LEVEL);
            System.exit(status);
        }

        try {
            String pdax = partitionDAX(mProps, mDAXFile, mDirectory, mType);
            mLogger.log("Partitioned DAX written out " + pdax, LogManager.CONSOLE_MESSAGE_LEVEL);
        } catch (Exception e) {
            mLogger.log("", e, LogManager.FATAL_MESSAGE_LEVEL);
            status = 1;
        }
        // log the end time and time execute
        double endtime = new Date().getTime();
        double execTime = (endtime - starttime) / 1000;
        mLogger.log(
                "Time taken to execute is " + execTime + " seconds", LogManager.INFO_MESSAGE_LEVEL);

        System.exit(status);
    }

    /**
     * @param properties the PegasusProperties
     * @param daxFile String
     * @param directory the directory where paritioned daxes reside
     * @param type the type of partitioning to use.
     * @return the path to the pdax file.
     */
    public String partitionDAX(
            PegasusProperties properties, String daxFile, String directory, String type) {
        int status = 0;
        // sanity check for the dax file
        if (daxFile == null || daxFile.length() == 0) {
            mLogger.log(
                    "The dax file that is to be partitioned not " + "specified",
                    LogManager.FATAL_MESSAGE_LEVEL);
            printShortVersion();
            status = 1;
            throw new RuntimeException("Unable to partition");
        }

        // always try to make the directory
        // referred to by the directory
        File dir = new File(directory);
        dir.mkdirs();

        // build up the partition graph
        // String callbackClass = ( type.equalsIgnoreCase("label") ) ?
        //                        "DAX2LabelGraph": //graph with labels populated
        //                        "DAX2Graph";

        // load the appropriate partitioner
        Callback callback = null;
        Partitioner partitioner = null;
        String daxName = null;
        int state = 0;
        try {
            PegasusBag bag = new PegasusBag();
            bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
            bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

            callback = DAXParserFactory.loadDAXParserCallback(type, bag, daxFile);

            // set the appropriate key that is to be used for picking up the labels
            if (callback instanceof DAX2LabelGraph) {
                ((DAX2LabelGraph) callback).setLabelKey(properties.getPartitionerLabelKey());
            }

            state = 1;
            XMLParser p = (XMLParser) DAXParserFactory.loadXMLDAXParser(bag, callback, daxFile);
            p.startParser(daxFile);

            state = 2;
            // get the graph map
            Map graphMap = (Map) callback.getConstructedObject();
            // get the fake dummy root node
            GraphNode root = (GraphNode) graphMap.get(DAX2Graph.DUMMY_NODE_ID);
            daxName = ((DAX2Graph) callback).getNameOfDAX();
            state = 3;
            partitioner = PartitionerFactory.loadInstance(properties, root, graphMap, type);
        } catch (FactoryException fe) {
            mLogger.log(fe.convertException(), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(2);
        } catch (Exception e) {
            int errorStatus = 1;
            switch (state) {
                case 0:
                    mLogger.log(
                            "Unable to load the DAXCallback", e, LogManager.FATAL_MESSAGE_LEVEL);
                    errorStatus = 2;
                    break;

                case 1:
                    mLogger.log(
                            "Error while parsing the DAX file", e, LogManager.FATAL_MESSAGE_LEVEL);
                    errorStatus = 1;
                    break;

                case 2:
                    mLogger.log(
                            "Error while determining the root of the parsed DAX",
                            e,
                            LogManager.FATAL_MESSAGE_LEVEL);
                    errorStatus = 1;
                    break;

                case 3:
                    mLogger.log(
                            "Unable to load the partitioner", e, LogManager.FATAL_MESSAGE_LEVEL);
                    errorStatus = 2;
                    break;

                default:
                    mLogger.log("Unknown Error", e, LogManager.FATAL_MESSAGE_LEVEL);
                    errorStatus = 1;
                    break;
            }
            status = errorStatus;
        }
        if (status > 0) {
            throw new RuntimeException("Unable to partition");
        }

        // load the writer callback that writes out
        // the partitioned daxes and PDAX
        WriterCallback cb = new WriterCallback();
        cb.initialize(properties, daxFile, daxName, directory);

        // start the partitioning of the graph
        partitioner.determinePartitions(cb);

        return cb.getPDAX();
    }

    /** Generates the short version of the help on the stdout. */
    public void printShortVersion() {
        String text =
                "\n $Id$ "
                        + "\n"
                        + getGVDSVersion()
                        + "\n Usage :partitiondax -d <dax file> [-D <dir for partitioned daxes>] "
                        + "   -t <type of partitioning to be used> [-c <path to property file>] [-v] [-V] [-h]";

        mLogger.log(text, LogManager.ERROR_MESSAGE_LEVEL);
    }

    /** Generated the long version of the help on the stdout. */
    public void printLongVersion() {
        String text =
                "\n "
                        + getGVDSVersion()
                        + "\n CPlanner/partitiondax - The tool that is used to partition the dax "
                        + "\n into smaller daxes for use in deferred planning."
                        + "\n "
                        + "\n Usage :partitiondax --dax <dax file> [--dir <dir for partitioned daxes>] "
                        + "\n --type <type of partitioning to be used> [--conf <path to property file>] [--verbose] [--version] "
                        + "\n [--help]"
                        + "\n"
                        + "\n Mandatory Options "
                        + "\n -d|--dax fn    the dax file that has to be partitioned into smaller daxes."
                        + "\n Other Options  "
                        + "\n -t|--type type the partitioning technique that is to be used for partitioning."
                        + "\n -D|--dir dir   the directory in which the partitioned daxes reside (defaults to "
                        + "\n                current directory)"
                        + "\n -c|--conf      path to  property file"
                        + "\n -v|--verbose   increases the verbosity of messages about what is going on."
                        + "\n -V|--version   displays the version number of the Griphyn Virtual Data System."
                        + "\n -h|--help      generates this help";

        System.out.println(text);
    }

    /**
     * Tt generates the LongOpt which contain the valid options that the command will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid options
     */
    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[7];

        longopts[0] = new LongOpt("dir", LongOpt.REQUIRED_ARGUMENT, null, 'D');
        longopts[1] = new LongOpt("dax", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[2] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');
        longopts[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        longopts[4] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        longopts[5] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[6] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        return longopts;
    }

    /** Loads all the properties that are needed by this class. */
    public void loadProperties() {}
}
