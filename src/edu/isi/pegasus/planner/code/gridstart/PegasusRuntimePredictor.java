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
package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.generator.Abstract;
import edu.isi.pegasus.planner.code.generator.condor.CondorGenerator;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Dagman;

import java.io.File;
import java.util.Collection;

/**
 * This class has convenience methods to create the argument invocation to pegasus-runtime-predictor
 * executable that gets invoked as a prescript with the job.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusRuntimePredictor {

    // $(wf_submit_dir)
    private static final String WF_SUBMIT_DIR_CONDOR_VARIABLE =
            "$(" + Condor.WF_SUBMIT_DIR_KEY + ")";

    /**
     * The suffix for the common log file in the workflow submit directory, to which all the
     * invocations of pegasus-runtime-predictor log.
     */
    public static final String LOG_SUFFIX = ".runtime-predictor.log";

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The path to the pegasus-runtime-predictor */
    protected String mPredictorPath;

    /** The path to the workflow file being planned. */
    protected String mWorkflowFile;

    /** The common log file, relative to the workflow submit directory. */
    protected String mLogFile;

    /** Defines the default encoding rules escape + with \+ encode single whitespace with + */
    public PegasusRuntimePredictor() {}

    /**
     * Initalizes the class
     *
     * @param bag the bag of initialization objects
     * @param dag the workflow being planned
     */
    public void initialize(PegasusBag bag, ADag dag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mPredictorPath = this.getDefaultRuntimePredictorPath();
        String dax = bag.getPlannerOptions().getDAX();
        mWorkflowFile = (dax == null) ? null : new File(dax).getAbsolutePath();
        mLogFile =
                Abstract.getDAGFilename(
                        bag.getPlannerOptions(), dag.getLabel(), dag.getIndex(), LOG_SUFFIX);
    }

    /**
     * Constructs the prescript that invokes
     *
     * <pre>pegasus-runtime-predictor</pre>
     *
     * on a job, as
     *
     * <pre>
     * pegasus-runtime-predictor --job-sub job.sub --level N --lof job.in.lof
     *                           --meta a.meta,b.meta -l wf.runtime-predictor.log
     *                           workflow.yml
     * </pre>
     *
     * @param job the <code>Job</code> object containing the job description of the job that has to
     *     be enabled on the grid.
     * @param inputLOF the input lof file for the job.
     * @param metaFiles collection of metafiles that will contain the sizes of the files
     */
    public void modify(Job job, String inputLOF, Collection<String> metaFiles) {
        // the key that indicates what executable to invoke as prescript.
        String prescriptKey = Dagman.PRE_SCRIPT_KEY;
        if (job.dagmanVariables.containsKey(prescriptKey)) {
            mLogger.log(
                    String.format(
                            "Job %s already has a prescript associated with it - %s",
                            job.getID(), job.dagmanVariables.get(prescriptKey)),
                    LogManager.WARNING_MESSAGE_LEVEL);
            return;
        }
        if (metaFiles.isEmpty()) {
            mLogger.log(
                    String.format(
                            "Job %s has no metafiles associated with it. Skipping"
                                    + " pegasus-runtime-prediction for it.",
                            job.getID()),
                    LogManager.WARNING_MESSAGE_LEVEL);
            return;
        }
        if (mWorkflowFile == null) {
            mLogger.log(
                    String.format(
                            "No workflow file to pass to pegasus-runtime-predictor. Skipping"
                                    + " pegasus-runtime-prediction for job %s",
                            job.getID()),
                    LogManager.WARNING_MESSAGE_LEVEL);
            return;
        }
        // the submit file for the job, relative to the workflow submit directory
        File jobSubmitFile =
                new File(
                        job.getRelativeSubmitDirectory(),
                        job.getFileBaseName(CondorGenerator.SUBMIT_FILE_SUFFIX));

        StringBuilder args = new StringBuilder();
        args.append("--job-sub").append(" ").append(jobSubmitFile.getPath()).append(" ");
        args.append("--level").append(" ").append(job.getLevel()).append(" ");
        args.append("--lof").append(" ").append(inputLOF).append(" ");

        if (!metaFiles.isEmpty()) {
            args.append("--meta").append(" ");
            for (String metaFile : metaFiles) {
                if (metaFile.startsWith(WF_SUBMIT_DIR_CONDOR_VARIABLE)) {
                    // dagman does not have a notion of variables for the
                    // dag file itself. So cannot expand $(wf_submit_dir).
                    // replace with a relative path
                    metaFile = metaFile.replace(WF_SUBMIT_DIR_CONDOR_VARIABLE, ".");
                }
                args.append(metaFile).append(",");
            }
            // remove trailing ,
            args.replace(args.length() - 1, args.length(), "");
        }

        args.append(" ").append("-l").append(" ").append(mLogFile);
        args.append(" ").append(mWorkflowFile);

        job.dagmanVariables.construct(prescriptKey, this.mPredictorPath);
        job.dagmanVariables.construct(Dagman.PRE_SCRIPT_ARGUMENTS_KEY, args.toString());
    }

    /**
     * Returns the path to pegasus-runtime-predictor executable that is used to predict the runtime
     * for a job.
     *
     * @return the path to the pegasus-runtime-predictor.
     */
    protected String getDefaultRuntimePredictorPath() {
        StringBuilder sb = new StringBuilder();
        sb.append(mProps.getBinDir());
        sb.append(File.separator).append("pegasus-runtime-predictor");
        return sb.toString();
    }
}
