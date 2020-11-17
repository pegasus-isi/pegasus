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
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * An Abstract Base class implementing the CodeGenerator interface. Introduces helper methods for
 * determining basenames of files, that contain concrete job descriptions.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public abstract class Abstract implements CodeGenerator {

    /** Suffix for the global log file to which exitcode should log to. */
    public static final String POSTSCRIPT_LOG_SUFFIX = ".exitcode.log";

    /** The bag of initialization objects. */
    protected PegasusBag mBag;

    /** The directory where all the submit files are to be generated. */
    protected String mSubmitFileDir;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The object containing the command line options specified to the planner at runtime. */
    protected PlannerOptions mPOptions;

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /**
     * Returns the name of the file on the basis of the metadata associated with the DAG. In case of
     * Condor dagman, it is the name of the .dag file that is written out. The basename of the .dag
     * file is dependant on whether the basename prefix has been specified at runtime or not by
     * command line options.
     *
     * @param dag the dag for which the .dag file has to be created.
     * @param suffix the suffix to be applied at the end.
     * @return the name of the dagfile.
     */
    protected String getDAGFilename(ADag dag, String suffix) {
        return getDAGFilename(mPOptions, dag.getLabel(), dag.getIndex(), suffix);
    }

    /**
     * Returns the name of the file on the basis of the metadata associated with the DAG. In case of
     * Condor dagman, it is the name of the .dag file that is written out. The basename of the .dag
     * file is dependant on whether the basename prefix has been specified at runtime or not by
     * command line options.
     *
     * @param options the options passed to the planner.
     * @param name the name attribute in dax
     * @param index the index attribute in dax.
     * @param suffix the suffix to be applied at the end.
     * @return the name of the dagfile.
     */
    public static String getDAGFilename(
            PlannerOptions options, String name, String index, String suffix) {
        // constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();
        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
        } else {
            // generate the prefix from the name of the dag
            sb.append(name).append("-").append(index);
        }
        // append the suffix
        sb.append(suffix);

        return sb.toString();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag the bag of initialization objects.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize(PegasusBag bag) throws CodeGeneratorException {
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger = bag.getLogger();
    }

    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon. The monitoring
     * should start only after the output files have been generated. FIXME: It should actually
     * happen after the workflow has been submitted. Eventually should be a separate monitor
     * interface, and submit writers should be loaded by an AbstractFactory.
     *
     * @return boolean indicating whether could successfully start the monitor daemon or not.
     */
    public boolean startMonitoring() {
        // by default not all code generators support monitoring.
        return false;
    }

    /**
     * Writes out the workflow metrics file for the workflow.
     *
     * @param workflow the workflow whose metrics file needs to be generated.
     */
    /*
        protected void writeOutWorkflowMetrics( ADag workflow ){


            try{
                Metrics metrics = new Metrics();
                metrics.initialize(mBag);

                Collection result = metrics.generateCode( workflow );
                for( Iterator it = result.iterator(); it.hasNext() ;){
                    mLogger.log("Written out workflow metrics file to " + it.next(), LogManager.DEBUG_MESSAGE_LEVEL);
                }
            }
            catch(CodeGeneratorException ioe){
                //log the message and return
                mLogger.log("Unable to write out the workflow metrics file ",
                            ioe, LogManager.ERROR_MESSAGE_LEVEL );
            }
        }
    */

    /**
     * Writes out the stampedeEventGenerator events for the workflow.
     *
     * @param workflow the workflow whose metrics file needs to be generated.
     */
    protected void writeOutStampedeEvents(ADag workflow) throws CodeGeneratorException {

        Stampede stampedeEventGenerator = new Stampede();
        stampedeEventGenerator.initialize(mBag);

        Collection result = stampedeEventGenerator.generateCode(workflow);
        for (Iterator it = result.iterator(); it.hasNext(); ) {
            mLogger.log(
                    "Written out stampede events for the executable workflow to " + it.next(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Writes out the metrics file for the workflow
     *
     * @param workflow the workflow whose metrics file needs to be generated.
     */
    protected void writeOutBraindump(ADag workflow) {

        // generate some extra keys for metrics file
        Map<String, String> entries = getAdditionalBraindumpEntries(workflow);

        try {
            Braindump braindump = new Braindump();
            braindump.initialize(mBag);

            Collection result = braindump.generateCode(workflow, entries);
            for (Iterator it = result.iterator(); it.hasNext(); ) {
                mLogger.log(
                        "Written out braindump to " + it.next(), LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } catch (CodeGeneratorException ioe) {
            // log the message and return
            mLogger.log(
                    "Unable to write out the braindump file for pegasus-monitord",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     * Writes out the DAX replica store
     *
     * @param workflow the work-flow
     */
    protected void writeOutDAXReplicaStore(ADag workflow) {
        try {
            DAXReplicaStore generator = new DAXReplicaStore();
            generator.initialize(mBag);

            Collection<File> result = generator.generateCode(workflow);
            for (File f : result) {
                mLogger.log(
                        "Written out dax replica store to " + f.getName(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } catch (CodeGeneratorException ioe) {
            // log the message and return
            mLogger.log(
                    "Unable to write out the notifications file ",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     * Writes out the generator input file for the work-flow.
     *
     * @param workflow the work-flow whose generator files needs to be generated.
     */
    protected void writeOutNotifications(ADag workflow) {
        try {
            MonitordNotify notifications = new MonitordNotify();
            notifications.initialize(mBag);

            Collection<File> result = notifications.generateCode(workflow);
            for (File f : result) {
                mLogger.log(
                        "Written out notifications to " + f.getName(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } catch (CodeGeneratorException ioe) {
            // log the message and return
            mLogger.log(
                    "Unable to write out the notifications file ",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     * Returns a Map containing additional metrics entries that are specific to a Code Generator
     *
     * @param workflow the workflow whose metrics file needs to be generated.
     * @return Map
     */
    public abstract Map<String, String> getAdditionalBraindumpEntries(ADag workflow);

    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset() throws CodeGeneratorException {
        mSubmitFileDir = null;
        mProps = null;
        mPOptions = null;
    }

    /**
     * Returns an open stream to the file that is used for writing out the job information for the
     * job.
     *
     * @param job the job whose job information needs to be written.
     * @param suffix
     * @return the writer to the open file.
     * @exception IOException if unable to open a write handle to the file.
     */
    public PrintWriter getWriter(Job job, String suffix) throws IOException {
        StringBuilder sb = new StringBuilder();
        // append the base name of the job
        sb.append(File.separatorChar).append(job.getFileFullPath(mSubmitFileDir, suffix));

        // intialize the print stream to the file
        return new PrintWriter(new BufferedWriter(new FileWriter(sb.toString())));
    }
}
