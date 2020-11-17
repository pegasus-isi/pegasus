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
import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.FactoryException;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A Central Properties class that keeps track of all the properties used by Pegasus. All other
 * classes access the methods in this class to get the value of the property. It access the
 * VDSProperties class to read the property file.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 * @see org.griphyn.common.util.VDSProperties
 */
public class VDS2PegasusProperties extends Executable {

    /** The handle to the internal map, that maps vds properties to pegasus properties. */
    private static Map mVDSToPegasusPropertiesTable;

    /**
     * An internal table that resolves the old transfer mode property, to the corresponding transfer
     * implementation.
     */
    private static Map mTXFERImplTable;

    /**
     * An internal table that resolves the old transfer mode property, to the corresponding transfer
     * refiner.
     */
    private static Map mTXFERRefinerTable;

    /** Store the regular expressions necessary to match the * properties. */
    private static final String mRegexExpression[] = {
        "(vds.replica.)([a-zA-Z_0-9]*[-]*)+(.prefer.stagein.sites)",
        "(vds.replica.)([a-zA-Z_0-9]*[-]*)+(.ignore.stagein.sites)",
        "(vds.site.selector.env.)([a-zA-Z_0-9]*[-]*)+",
        "(vds.exitcode.path.)([a-zA-Z_0-9]*[-]*)+",
        "(vds.partitioner.horizontal.bundle.)([a-zA-Z_0-9]*[-]*)+",
        "(vds.partitioner.horizontal.collapse.)([a-zA-Z_0-9]*[-]*)+",
        "(vds.transfer.rft.)([a-zA-Z_0-9]*[-]*)+",
        "(vds.transfer.crft.)([a-zA-Z_0-9]*[-]*)+",
        // "(vds.db.)([a-zA-Z_0-9]*[-]*)+(.)([a-zA-Z_0-9.]*[-]*)+"
        "(vds.db.tc.driver)[.]+([a-zA-Z_0-9]*[-]*)+",
        "(vds.db.ptc.driver)[.]+([a-zA-Z_0-9]*[-]*)+",
        "(vds.db.\\*.driver)[.]+([a-zA-Z_0-9]*[-]*)+",
    };

    /** Replacement 2 D Array for the above properties. */
    private static final String mStarReplacements[][] = {
        {"vds.replica.", "pegasus.selector.replica."},
        {"vds.replica.", "pegasus.selector.replica."},
        {"vds.site.selector.env.", "pegasus.selector.site.env."},
        {"vds.exitcode.path.", "pegasus.exitcode.path."},
        {"vds.partitioner.horizontal.bundle", "pegasus.partitioner.horizontal.bundle."},
        {"vds.partitioner.horizontal.collapse", "pegasus.partitioner.horizontal.collapse."},
        {"vds.transfer.rft.", "pegasus.transfer.rft."},
        {"vds.transfer.crft.", "pegasus.transfer.crft."},
        // { "vds.db.", "pegasus.db." }
        {"vds.db.tc.driver.", "pegasus.catalog.transformation.db."},
        {"vds.db.ptc.driver.", "pegasus.catalog.provenance.db."},
        {"vds.db.\\*.driver.", "pegasus.catalog.*.db."},
    };

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private static Pattern mCompiledPatterns[] = null;

    /** The input directory containing the kickstart records. */
    private String mInputFile;

    /** The output directory where to generate the ploticus output. */
    private String mOutputDir;

    /** The default constructor. Compiles the patterns only once. */
    public VDS2PegasusProperties() {
        // initialize the compiled expressions once
        if (mCompiledPatterns == null) {
            mCompiledPatterns = new Pattern[mRegexExpression.length];
            for (int i = 0; i < mRegexExpression.length; i++)
                mCompiledPatterns[i] = Pattern.compile(mRegexExpression[i]);
        }
    }

    public void initialize(String[] opts) {
        super.initialize(opts);
    }

    /**
     * Singleton access to the transfer implementation table. Contains the mapping of the old
     * transfer property value to the new transfer implementation property value.
     *
     * @return map
     */
    private static Map transferImplementationTable() {
        // singleton access
        if (mTXFERImplTable == null) {
            mTXFERImplTable = new HashMap(13);
            mTXFERImplTable.put("Bundle", "Transfer");
            mTXFERImplTable.put("Chain", "Transfer");
            mTXFERImplTable.put("CRFT", "CRFT");
            mTXFERImplTable.put("GRMS", "GRMS");
            mTXFERImplTable.put("multiple", "Transfer");
            mTXFERImplTable.put("Multiple", "Transfer");
            mTXFERImplTable.put("MultipleTransfer", "Transfer");
            mTXFERImplTable.put("RFT", "RFT");
            mTXFERImplTable.put("single", "OldGUC");
            mTXFERImplTable.put("Single", "OldGUC");
            mTXFERImplTable.put("SingleTransfer", "OldGUC");
            mTXFERImplTable.put("StorkSingle", "Stork");
            mTXFERImplTable.put("T2", "T2");
        }
        return mTXFERImplTable;
    }

    /**
     * Singleton access to the transfer refiner table. Contains the mapping of the old transfer
     * property value to the new transfer refiner property value.
     *
     * @return map
     */
    private static Map transferRefinerTable() {
        // singleton access
        if (mTXFERRefinerTable == null) {
            mTXFERRefinerTable = new HashMap(13);
            mTXFERRefinerTable.put("Bundle", "Bundle");
            mTXFERRefinerTable.put("Chain", "Chain");
            mTXFERRefinerTable.put("CRFT", "Default");
            mTXFERRefinerTable.put("GRMS", "GRMS");
            mTXFERRefinerTable.put("multiple", "Default");
            mTXFERRefinerTable.put("Multiple", "Default");
            mTXFERRefinerTable.put("MultipleTransfer", "Default");
            mTXFERRefinerTable.put("RFT", "Default");
            mTXFERRefinerTable.put("single", "SDefault");
            mTXFERRefinerTable.put("Single", "SDefault");
            mTXFERRefinerTable.put("SingleTransfer", "SDefault");
            mTXFERRefinerTable.put("StorkSingle", "Single");
            mTXFERRefinerTable.put("T2", "Default");
        }
        return mTXFERRefinerTable;
    }

    /**
     * Singleton access to the transfer implementation table. Contains the mapping of the old
     * transfer property value to the new transfer implementation property value.
     *
     * @return map
     */
    private static Map vdsToPegasusPropertiesTable() {
        // return the already existing one if possible
        if (mVDSToPegasusPropertiesTable != null) {
            return mVDSToPegasusPropertiesTable;
        }

        // PROPERTIES RELATED TO SCHEMAS
        associate("vds.schema.dax", "pegasus.schema.dax");
        associate("vds.schema.pdax", "pegasus.schema.pdax");
        associate("vds.schema.poolconfig", "pegasus.schema.sc");
        associate("vds.schema.sc", "pegasus.schema.sc");
        associate("vds.db.ptc.schema", "pegasus.catalog.provenance");

        // PROPERTIES RELATED TO DIRECTORIES
        associate("vds.dir.exec", "pegasus.dir.exec");
        associate("vds.dir.storage", "pegasus.dir.storage");
        associate("vds.dir.create.mode", "pegasus.dir.create");
        associate("vds.dir.create", "pegasus.dir.create");
        associate("vds.dir.timestamp.extended", "pegasus.dir.timestamp.extended");

        // PROPERTIES RELATED TO THE TRANSFORMATION CATALOG
        associate("vds.tc.mode", "pegasus.catalog.transformation");
        associate("vds.tc", "pegasus.catalog.transformation");
        associate("vds.tc.file", "pegasus.catalog.transformation.file");
        associate("vds.tc.mapper", "pegasus.catalog.transformation.mapper");

        // REPLICA CATALOG PROPERTIES
        associate("vds.replica.mode", "pegasus.catalog.replica");
        associate("vds.rc", "pegasus.catalog.replica");
        associate("vds.rls.url", "pegasus.catalog.replica.url");
        associate("vds.rc.url", "pegasus.catalog.replica.url");
        associate("vds.rc.lrc.ignore", "pegasus.catalog.replica.lrc.ignore");
        associate("vds.rc.lrc.restrict", "pegasus.catalog.replica.lrc.restrict");
        associate("vds.cache.asrc", "pegasus.catalog.replica.cache.asrc");
        associate("vds.rls.query", "");
        associate("vds.rls.query.attrib", "");
        associate("vds.rls.exit", "");
        associate("vds.rc.rls.timeout", "");

        // SITE CATALOG PROPERTIES
        associate("vds.pool.mode", "pegasus.catalog.site");
        associate("vds.sc", "pegasus.catalog.site");
        associate("vds.pool.file", "pegasus.catalog.site.file");
        associate("vds.sc.file", "pegasus.catalog.site.file");

        // PROPERTIES RELATED TO SELECTION
        associate("vds.transformation.selector", "pegasus.selector.transformation");

        associate("vds.rc.selector", "pegasus.selector.replica");
        associate("vds.replica.selector", "pegasus.selector.replica");
        //        associate( "vds.replica.*.prefer.stagein.sites",
        // "pegasus.selector.replica.*.prefer.stagein.sites" );
        associate("vds.rc.restricted.sites", "pegasus.selector.replica.*.ignore.stagein.sites");
        //        associate( "vds.replica.*.ignore.stagein.sites",
        // "pegasus.selector.replica.*.ignore.stagein.sites" );

        associate("vds.site.selector", "pegasus.selector.site");
        associate("vds.site.selector.path", "pegasus.selector.site.path");
        associate("vds.site.selector.timeout", "pegasus.selector.site.timeout");
        associate("vds.site.selector.keep.tmp", "pegasus.selector.site.keep.tmp");

        // TRANSFER MECHANISM PROPERTIES
        associate("vds.transfer.*.impl", "pegasus.transfer.*.impl");
        associate("vds.transfer.stagein.impl", "pegasus.transfer.stagein.impl");
        associate("vds.transfer.stageout.impl", "pegasus.transfer.stageout.impl");
        associate("vds.transfer.stagein.impl", "pegasus.transfer.inter.impl");
        associate("vds.transfer.refiner", "pegasus.transfer.refiner");
        associate("vds.transfer.single.quote", "pegasus.transfer.single.quote");
        associate("vds.transfer.throttle.processes", "pegasus.transfer.throttle.processes");
        associate("vds.transfer.throttle.streams", "pegasus.transfer.throttle.streams");
        associate("vds.transfer.force", "pegasus.transfer.force");
        associate("vds.transfer.mode.links", "pegasus.transfer.links");
        associate("vds.transfer.links", "pegasus.transfer.links");
        associate("vds.transfer.thirdparty.sites", "pegasus.transfer.*.thirdparty.sites");
        associate("vds.transfer.thirdparty.pools", "pegasus.transfer.*.thirdparty.sites");
        associate("vds.transfer.*.thirdparty.sites", "pegasus.transfer.*.thirdparty.sites");
        associate(
                "vds.transfer.stagein.thirdparty.sites",
                "pegasus.transfer.stagein.thirdparty.sites");
        associate(
                "vds.transfer.stageout.thirdparty.sites",
                "pegasus.transfer.stageout.thirdparty.sites");
        associate("vds.transfer.inter.thirdparty.sites", "pegasus.transfer.inter.thirdparty.sites");
        associate("vds.transfer.staging.delimiter", "pegasus.transfer.staging.delimiter");
        associate("vds.transfer.disable.chmod.sites", "pegasus.transfer.disable.chmod.sites");
        associate("vds.transfer.proxy", "pegasus.transfer.proxy");
        associate("vds.transfer.arguments", "pegasus.transfer.arguments");
        associate("vds.transfer.*.priority", "pegasus.transfer.*.priority");
        associate("vds.transfer.stagein.priority", "pegasus.transfer.stagein.priority");
        associate("vds.transfer.stageout.priority", "pegasus.transfer.stageout.priority");
        associate("vds.transfer.inter.priority", "pegasus.transfer.inter.priority");
        associate("vds.scheduler.stork.cred", "pegasus.transfer.stork.cred");

        // PROPERTIES RELATED TO KICKSTART AND EXITCODE
        associate("vds.gridstart", "pegasus.gridstart");
        associate("vds.gridstart.invoke.always", "pegasus.gristart.invoke.always");
        associate("vds.gridstart.invoke.length", "pegasus.gridstart.invoke.length");
        associate("vds.gridstart.kickstart.stat", "pegasus.gridstart.kickstart.stat");
        associate("vds.gridstart.label", "pegasus.gristart.label");

        associate("vds.exitcode.impl", "pegasus.exitcode.impl");
        associate("vds.exitcode.mode", "pegasus.exitcode.scope");
        associate("vds.exitcode", "pegasus.exitcode.scope");
        //        associate( "vds.exitcode.path.[value]","pegasus.exitcode.path.[value]" );
        associate("vds.exitcode.arguments", "pegasus.exitcode.arguments");
        associate("vds.exitcode.debug", "pegasus.exitcode.debug");
        associate("vds.prescript.arguments", "pegasus.prescript.arguments");

        // PROPERTIES RELATED TO REMOTE SCHEDULERS
        associate("vds.scheduler.remote.projects", "pegasus.remote.scheduler.projects");
        associate("vds.scheduler.remote.queues", "pegasus.remote.scheduler.queues");
        //        associate( "vds.scheduler.remote.maxwalltimes",
        // "pegasus.remote.scheduler.maxwalltimes" );
        associate("vds.scheduler.remote.min.maxtime", "pegasus.remote.scheduler.min.maxtime");
        associate(
                "vds.scheduler.remote.min.maxwalltime", "pegasus.remote.scheduler.min.maxwalltime");
        associate("vds.scheduler.remote.min.maxcputime", "pegasus.remote.scheduler.min.maxcputime");

        // PROPERTIES RELATED TO Condor and DAGMAN
        associate("vds.scheduler.condor.release", "pegasus.condor.release");
        associate("vds.scheduler.condor.remove", "pegasus.condor.remove");
        associate("vds.scheduler.condor.arguments.quote", "pegasus.condor.arguments.quote");
        associate("vds.scheduler.condor.output.stream", "pegasus.condor.output.stream");
        associate("vds.scheduler.condor.error.stream", "pegasus.condor.error.stream");
        associate("vds.scheduler.condor.retry", "pegasus.dagman.retry");

        // JOB CLUSTERING
        associate("vds.exec.node.collapse", "pegasus.clusterer.nodes");
        associate("vds.job.aggregator", "pegasus.clusterer.job.aggregator");
        associate(
                "vds.job.aggregator.seqexec.isgloballog",
                "pegasus.clusterer.job.aggregator.hasgloballog");
        associate("vds.clusterer.label.key", "pegasus.clusterer.label.key");

        // MISCELLANEOUS
        associate("vds.auth.gridftp.timeout", "pegasus.auth.gridftp.timeout");
        associate("vds.submit.mode", "pegasus.submit");
        associate("vds.job.priority", "pegasus.job.priority");

        associate("vds.dax.callback", "pegasus.parser.dax.callback");
        associate("vds.label.key", "pegasus.partitioner.label.key");
        associate("vds.partitioner.label.key", "pegasus.partitioner.label.key");
        associate("vds.partition.parser.mode", "pegasus.partitioner.parser.load");
        //        associate( "vds.partitioner.horizontal.bundle.",
        // "pegasus.partitioner.horizontal.bundle." );
        //        associate( "vds.partitioner.horizontal.collapse.",
        // "pegasus.partitioner.horizontal.collapse." );

        // SOME DB DRIVER PROPERTIES
        associate("vds.db.*.driver", "pegasus.catalog.*.db.driver");
        associate("vds.db.tc.driver", "pegasus.catalog.transformation.db.driver");
        associate("vds.db.ptc.driver", "pegasus.catalog.provenance.db.driver");

        // WORK DB PROPERTIES
        associate("work.db", "pegasus.catalog.work.db");
        associate("work.db.hostname", "pegasus.catalog.work.db.hostname");
        associate("work.db.database", "pegasus.catalog.work.db.database");
        associate("work.db.user", "pegasus.catalog.work.db.user");
        associate("work.db.password", "pegasus.catalog.work.db.password");

        return mVDSToPegasusPropertiesTable;
    }

    /**
     * Convert a VDS Properties file to Pegasus properties.
     *
     * @param input the path to the VDS Properties file.
     * @param directory the directory where the Pegasus properties file needs to be written out to.
     * @return path to the properties file that is written.
     * @exception IOException
     */
    public String convert(String input, String directory) throws IOException {
        File dir = new File(directory);

        // sanity check on the directory
        sanityCheck(dir);

        // we only want to write out the VDS properties for time being
        Properties ipProperties = new Properties();
        ipProperties.load(new FileInputStream(input));
        Properties vdsProperties = this.matchingSubset(ipProperties, "vds", true);

        // traverse through the VDS properties and convert them to
        // the new names
        Properties temp = new Properties();
        for (Iterator it = vdsProperties.keySet().iterator(); it.hasNext(); ) {
            String vds = (String) it.next();
            String vdsValue = (String) vdsProperties.get(vds);
            String pgs = (String) vdsToPegasusPropertiesTable().get(vds);

            // if pgs is not null store the pgs with the vds value
            // if null then barf
            if (pgs == null) {
                // match for star properties
                pgs = matchForStarProperties(vds);
                if (pgs == null) {
                    System.err.println("Unable to associate VDS property " + vds);
                    continue;
                }
            } else {
                if (pgs.length() == 0) {
                    // ignore
                    continue;
                }
            }
            // put the pegasus property with the vds value
            temp.setProperty(pgs, vdsValue);
        }

        // put the properties in temp into PegasusProperties in a sorted order
        // does not work, as the store method does not store it in that manner
        Map pegasusProperties = new TreeMap();
        for (Iterator it = temp.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            pegasusProperties.put(key, (String) temp.get(key));
        }

        // create a temporary file in directory
        File f = File.createTempFile("pegasus.", ".properties", dir);
        PrintWriter pw = new PrintWriter(new FileWriter(f));

        // the header of the file
        StringBuffer header = new StringBuffer(64);
        header.append(
                "############################################################################\n");
        header.append("# PEGASUS USER PROPERTIES GENERATED FROM VDS PROPERTY FILE \n")
                .append("# ( " + input + " ) \n")
                .append("# GENERATED AT ")
                .append(Currently.iso8601(false, true, false, new java.util.Date()))
                .append("\n");
        header.append(
                "############################################################################");

        pw.println(header.toString());

        for (Iterator it = pegasusProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            String line = entry.getKey() + " = " + entry.getValue();
            pw.println(line);
        }
        pw.close();

        /*
        //the header of the file
        StringBuffer header = new StringBuffer(64);
        header.append( "############################################################################\n" );
        header.append( "# PEGASUS USER PROPERTIES GENERATED FROM VDS PROPERTY FILE \n#( " + input + " ) \n" )
              .append( "# ESCAPES IN VALUES ARE INTRODUCED \n");
        header.append( "############################################################################" );


        //create an output stream to this file and write out the properties
        OutputStream os = new FileOutputStream( f );
        pegasusProperties.store( os, header.toString() );
        os.close();


        //convert the properties file into a sorted properties file
        convertToSorted( f, dir );
        */

        return f.getAbsolutePath();
    }

    /**
     * Returns a matching pegasus property for a VDS star property.
     *
     * @param vds the vds property.
     * @return the new Pegasus Property if found else, null.
     */
    protected String matchForStarProperties(String vds) {
        String pgs = null;

        // match against pattern
        for (int i = 0; i < mRegexExpression.length; i++) {
            // if a vds property matches against existing patterns
            if (mCompiledPatterns[i].matcher(vds).matches()) {
                // get the replacement value
                pgs = vds.replaceFirst(mStarReplacements[i][0], mStarReplacements[i][1]);
                System.out.println("The matching pegasus * property for " + vds + " is " + pgs);
                break;
            }
        }
        return pgs;
    }

    /**
     * The main test program.
     *
     * @param args the arguments to the program.
     */
    public static void main(String[] args) {
        VDS2PegasusProperties me = new VDS2PegasusProperties();
        int result = 0;

        try {
            me.initialize(args);
            me.executeCommand();
        } catch (FactoryException fe) {
            me.log(fe.convertException(), LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        } catch (RuntimeException rte) {
            // catch all runtime exceptions including our own that
            // are thrown that may have chained causes
            me.log(convertException(rte, me.mLogger.getLevel()), LogManager.FATAL_MESSAGE_LEVEL);
            result = 1;
        } catch (Exception e) {
            // unaccounted for exceptions
            me.log(e.getMessage(), LogManager.FATAL_MESSAGE_LEVEL);
            e.printStackTrace();
            result = 3;
        }

        // warn about non zero exit code
        if (result != 0) {
            me.log("Non-zero exit-code " + result, LogManager.WARNING_MESSAGE_LEVEL);
        }

        System.exit(result);
    }

    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     */
    public void executeCommand() {
        parseCommandLineArguments(getCommandLineOptions());

        // sanity check on output directory
        mOutputDir = (mOutputDir == null) ? "." : mOutputDir;
        File dir = new File(mOutputDir);
        if (dir.exists()) {
            // directory already exists.
            if (dir.isDirectory()) {
                if (!dir.canWrite()) {
                    throw new RuntimeException(
                            "Cannot write out to output directory " + mOutputDir);
                }
            } else {
                // directory is a file
                throw new RuntimeException(mOutputDir + " is not a directory ");
            }

        } else {
            dir.mkdirs();
        }

        String output;
        try {
            output = this.convert(mInputFile, mOutputDir);
            System.out.println("Pegasus Properties Written out to file " + output);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to convert properties file ", ioe);
        }
    }

    /**
     * Parses the command line arguments using GetOpt and returns a <code>PlannerOptions</code>
     * contains all the options passed by the user at the command line.
     *
     * @param args the arguments passed by the user at command line.
     */
    public void parseCommandLineArguments(String[] args) {
        LongOpt[] longOptions = generateValidOptions();

        Getopt g = new Getopt("properties-converter", args, "i:o:c:h", longOptions, false);
        g.setOpterr(false);

        int option = 0;

        while ((option = g.getopt()) != -1) {
            // System.out.println("Option tag " + (char)option);
            switch (option) {
                case 'i': // input
                    this.mInputFile = g.getOptarg();
                    break;

                case 'h': // help
                    printLongVersion();
                    System.exit(0);
                    return;

                case 'o': // output directory
                    this.mOutputDir = g.getOptarg();
                    break;
                case 'c':
                    // do nothing
                    break;

                default: // same as help
                    printShortVersion();
                    throw new RuntimeException(
                            "Incorrect option or option usage " + (char) g.getOptopt());
            }
        }
    }

    /**
     * Tt generates the LongOpt which contain the valid options that the command will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid options
     */
    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[4];

        longopts[0] = new LongOpt("input", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        longopts[1] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[3] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        return longopts;
    }

    /** Prints out a short description of what the command does. */
    public void printShortVersion() {
        String text =
                "\n $Id$ "
                        + "\n "
                        + getGVDSVersion()
                        + "\n Usage : properties-converter [-Dprop  [..]] -i <input directory>  "
                        + " [-o output directory]  [-c <path to property file>] [-h]";

        System.out.println(text);
    }

    /**
     * Prints the long description, displaying in detail what the various options to the command
     * stand for.
     */
    public void printLongVersion() {

        String text =
                "\n $Id$ "
                        + "\n "
                        + getGVDSVersion()
                        + "\n properties-converter - A tool that converts the VDS properties file to "
                        + "\n                         the corresponding Pegasus properties file "
                        + "\n Usage: properties-converter [-Dprop  [..]] --input <input file> "
                        + "\n        [--output output directory] [--conf <path to property file>]  [--help] "
                        + "\n"
                        + "\n Mandatory Options "
                        + "\n --input              the path to the VDS properties file."
                        + "\n Other Options  "
                        + "\n -o |--output        the output directory where to generate the pegasus property file."
                        + "\n -c |--conf          path to  property file"
                        + "\n -h |--help          generates this help."
                        + "\n ";

        System.out.println(text);
        // mLogger.log(text,LogManager.INFO_MESSAGE_LEVEL);
    }

    /** Loads all the properties that would be needed by the Toolkit classes. */
    public void loadProperties() {
        // empty for time being
    }

    /**
     * Returns the transfer implementation.
     *
     * @param property property name.
     * @return the transfer implementation, else the one specified by "pegasus.transfer.*.impl",
     *     else the DEFAULT_TRANSFER_IMPLEMENTATION.
     */
    /*
     public String getTransferImplementation(String property){
         String value = mProps.getProperty(property,
                                           getDefaultTransferImplementation());

         if(value == null){
            //check for older deprecated properties
            value = mProps.getProperty("pegasus.transfer");
            value = (value == null)?
                    mProps.getProperty("pegasus.transfer.mode"):
                    value;

            //convert a non null value to the corresponding
            //transfer implementation
            if(value != null){
                value = (String)transferImplementationTable().get(value);
                logDeprecatedWarning("pegasus.transfer","pegasus.transfer.*.impl and " +
                                     "pegasus.transfer.refiner");
            }

        }

        //put in default if still we have a non null
        value = (value == null)?
                DEFAULT_TRANSFER_IMPLEMENTATION:
                value;
        return value;
    }
    */

    /**
     * Returns the transfer refiner that is to be used for adding in the transfer jobs in the
     * workflow
     *
     * <p>Referred to by the "pegasus.transfer.refiner" property.
     *
     * @return the transfer refiner, else the DEFAULT_TRANSFER_REFINER.
     * @see #DEFAULT_TRANSFER_REFINER
     */
    /*
    public String getTransferRefiner(){
        String value = mProps.getProperty("pegasus.transfer.refiner");
        if(value == null){
            //check for older deprecated properties
            value = mProps.getProperty("pegasus.transfer");
            value = (value == null)?
                    mProps.getProperty("pegasus.transfer.mode"):
                    value;

            //convert a non null value to the corresponding
            //transfer refiner
            if(value != null){
                value = (String)transferRefinerTable().get(value);
                logDeprecatedWarning("pegasus.transfer","pegasus.transfer.impl and " +
                                     "pegasus.transfer.refiner");
            }
        }

        //put in default if still we have a non null
        value = (value == null)?
                DEFAULT_TRANSFER_REFINER:
                value;
        return value;

    }
    */

    // SOME LOGGING PROPERTIES

    /**
     * Returns the file to which all the logging needs to be directed to.
     *
     * <p>Referred to by the "vds.log.*" property.
     *
     * @return the value of the property that is specified, else null
     */
    //    public String getLoggingFile(){
    //        return mProps.getProperty("vds.log.*");
    //    }

    /**
     * Returns the location of the local log file where you want the messages to be logged. Not used
     * for the moment.
     *
     * <p>Referred to by the "vds.log4j.log" property.
     *
     * @return the value specified in the property file,else null.
     */
    //    public String getLog4JLogFile() {
    //        return getProperty( "vds.log.file", "vds.log4j.log" );
    //    }

    /**
     * Return returns the environment string specified for the local pool. If specified the
     * registration jobs are set with these environment variables.
     *
     * <p>Referred to by the "vds.local.env" property
     *
     * @return the environment string for local pool in properties file if defined, else null.
     */
    //    public String getLocalPoolEnvVar() {
    //        return mProps.getProperty( "vds.local.env" );
    //    }

    /**
     * Returns a boolean indicating whether to treat the entries in the cache files as a replica
     * catalog or not.
     *
     * @return boolean
     */
    //    public boolean treatCacheAsRC(){
    //        return Boolean.parse(mProps.getProperty( "vds.cache.asrc"),
    //                             false);
    //    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck(File dir) throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory " + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException(
                        "Destination "
                                + dir.getPath()
                                + " already "
                                + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create directory destination " + dir.getPath());
            }
        }
    }

    /**
     * Extracts a specific property key subset from the known properties. The prefix may be removed
     * from the keys in the resulting dictionary, or it may be kept. In the latter case, exact
     * matches on the prefix will also be copied into the resulting dictionary.
     *
     * @param properties is the properties from where to get the subset.
     * @param prefix is the key prefix to filter the properties by.
     * @param keepPrefix if true, the key prefix is kept in the resulting dictionary. As
     *     side-effect, a key that matches the prefix exactly will also be copied. If false, the
     *     resulting dictionary's keys are shortened by the prefix. An exact prefix match will not
     *     be copied, as it would result in an empty string key.
     * @return a property dictionary matching the filter key. May be an empty dictionary, if no
     *     prefix matches were found.
     */
    public Properties matchingSubset(Properties properties, String prefix, boolean keepPrefix) {
        Properties result = new Properties();

        // sanity check
        if (prefix == null || prefix.length() == 0) return result;

        String prefixMatch; // match prefix strings with this
        String prefixSelf; // match self with this
        if (prefix.charAt(prefix.length() - 1) != '.') {
            // prefix does not end in a dot
            prefixSelf = prefix;
            prefixMatch = prefix + '.';
        } else {
            // prefix does end in one dot, remove for exact matches
            prefixSelf = prefix.substring(0, prefix.length() - 1);
            prefixMatch = prefix;
        }
        // POSTCONDITION: prefixMatch and prefixSelf are initialized!

        // now add all matches into the resulting properties.
        // Remark 1: #propertyNames() will contain the System properties!
        // Remark 2: We need to give priority to System properties. This is done
        // automatically by calling this class's getProperty method.
        String key;
        for (Enumeration e = properties.propertyNames(); e.hasMoreElements(); ) {
            key = (String) e.nextElement();

            if (keepPrefix) {
                // keep full prefix in result, also copy direct matches
                if (key.startsWith(prefixMatch) || key.equals(prefixSelf))
                    result.setProperty(key, (String) properties.get(key));
            } else {
                // remove full prefix in result, dont copy direct matches
                if (key.startsWith(prefixMatch))
                    result.setProperty(
                            key.substring(prefixMatch.length()), (String) properties.get(key));
            }
        }

        // done
        return result;
    }

    /**
     * Associates a VDS property with the new pegasus property.
     *
     * @param vdsProperty the old VDS property.
     * @param pegasusProperty the new Pegasus property.
     */
    private static void associate(String vdsProperty, String pegasusProperty) {
        if (mVDSToPegasusPropertiesTable == null) {
            mVDSToPegasusPropertiesTable = new HashMap(13);
        }

        mVDSToPegasusPropertiesTable.put(vdsProperty, pegasusProperty);
    }
}
