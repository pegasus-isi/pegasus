/**
 * Copyright 2007-2014 University Of Southern California
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
package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.generator.Metrics;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Namespace;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A helper class, that mimics the functionality of pegasus-submit-dag, and generates a condor.sub
 * for dagman using the condor_submit_dag -nosubmit option.
 *
 * @author Karan Vahi
 */
public class PegasusSubmitDAG {

    /**
     * The dagman knobs controlled through property. They map the property name to the corresponding
     * dagman option.
     */
    public static final String DAGMAN_KNOBS[][] = {
        {Dagman.MAXPRE_KEY.toLowerCase(), " -MaxPre "},
        {Dagman.MAXPOST_KEY.toLowerCase(), " -MaxPost "},
        {Dagman.MAXJOBS_KEY.toLowerCase(), " -MaxJobs "},
        {Dagman.MAXIDLE_KEY.toLowerCase(), " -MaxIdle "},
    };

    /** Default number of max postscripts run by dagman at a time. */
    public static final int DEFAULT_MAX_POST_SCRIPTS = 20;

    /** The Bag of Pegasus initialization objects. */
    private PegasusBag mBag;

    private LogManager mLogger;

    private PegasusProperties mProps;
    private PlannerOptions mPOptions;

    /** The local environment as picked up from the environment, properties and the site catalog */
    private Namespace mLocalEnv;

    public PegasusSubmitDAG() {}

    public void intialize(PegasusBag bag) {
        mBag = bag;
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();

        mLocalEnv = new ENV();
        Map<String, String> systemEnv = System.getenv();
        for (Map.Entry<String, String> entry : systemEnv.entrySet()) {
            mLocalEnv.construct(entry.getKey(), entry.getValue());
        }
        mLocalEnv.assimilate(mProps, Profiles.NAMESPACES.env);
        // override them from the local site catalog entry
        SiteStore store = bag.getHandleToSiteStore();
        if (store != null && store.contains("local")) {
            SiteCatalogEntry site = store.lookup("local");
            Namespace localSiteEnv = site.getProfiles().get(Profiles.NAMESPACES.env);
            for (Iterator<String> it = localSiteEnv.getProfileKeyIterator(); it.hasNext(); ) {
                String key = it.next();
                mLocalEnv.checkKeyInNS(key, (String) localSiteEnv.get(key));
            }
        }
    }

    /**
     * @param dag the executable workflow.
     * @param dagFile
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag, File dagFile) throws CodeGeneratorException {
        Collection<File> result = new ArrayList();

        // find path to condor_submit_dag
        File condorSubmitDAG = FindExecutable.findExec("condor_submit_dag");
        if (condorSubmitDAG == null) {
            throw new CodeGeneratorException("Unable to find path to condor_submit_dag");
        }

        // construct arguments for condor_submit_dag
        String args = getCondorSubmitDagArgs(dag, dagFile);

        try {
            // set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();
            String invocation = condorSubmitDAG.getAbsolutePath() + " " + args;

            // PM-921 all the local env profiles get rendered as String[]
            String[] env = new String[mLocalEnv.size()];
            int i = 0;
            for (Iterator<String> it = mLocalEnv.getProfileKeyIterator(); it.hasNext(); i++) {
                String key = it.next();
                StringBuilder sb = new StringBuilder();
                sb.append(key).append("=").append((String) mLocalEnv.get(key));
                env[i] = sb.toString();
            }
            mLogger.log(
                    "Executing  " + invocation + " with " + mLocalEnv,
                    LogManager.DEBUG_MESSAGE_LEVEL);

            Process p = r.exec(invocation, null, dagFile.getParentFile());

            // spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                    new StreamGobbler(
                            p.getInputStream(),
                            new PSDStreamGobblerCallback(LogManager.CONSOLE_MESSAGE_LEVEL));
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new PSDStreamGobblerCallback(LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            // get the status
            int status = p.waitFor();

            mLogger.log(
                    "condor_submit_dag exited with status " + status,
                    LogManager.DEBUG_MESSAGE_LEVEL);

            if (status != 0) {
                throw new CodeGeneratorException(
                        "Command failed with non zero exit status " + invocation);
            }
        } catch (IOException ioe) {
            mLogger.log(
                    "IOException while running condor_submit_dag ",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
            throw new CodeGeneratorException("IOException while running condor_submit_dag ", ioe);
        } catch (InterruptedException ie) {
            // ignore
        }

        // we have the .condor.sub file now.
        File dagSubmitFile = new File(dagFile.getAbsolutePath() + ".condor.sub");
        // sanity check to ensure no disconnect
        if (!dagSubmitFile.canRead()) {
            throw new CodeGeneratorException(
                    "Unable to read the dagman condor submit file " + dagSubmitFile);
        }

        if (!modifyDAGManSubmitFileForMetrics(dagSubmitFile)) {
            mLogger.log(
                    "DAGMan metrics reporting not enabled for dag " + dagFile,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;
    }

    /**
     * Modifies the dagman condor submit file for metrics reporting.
     *
     * @param file
     * @return true if file is modified, else false
     * @throws CodeGeneratorException
     */
    protected boolean modifyDAGManSubmitFileForMetrics(File file) throws CodeGeneratorException {
        // modify the environment string to add the environment for
        // enabling DAGMan metrics if so required.
        Metrics metricsReporter = new Metrics();
        metricsReporter.initialize(mBag);
        ENV env = metricsReporter.getDAGManMetricsEnv();
        if (env.isEmpty()) {
            return false;
        } else {
            // we read the DAGMan submit file in and grab the environment from it
            // and add the environment key to the second last line with the
            // Pegasus metrics environment variables added.
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                String dagmanEnvString = "";
                String line = null;
                long previous = raf.getFilePointer();
                while ((line = raf.readLine()) != null) {
                    if (line.startsWith("environment")) {
                        dagmanEnvString = line;
                    }
                    if (line.startsWith("queue")) {
                        // backtrack to previous file position i.e just before queue
                        raf.seek(previous);
                        StringBuilder dagmanEnv = new StringBuilder(dagmanEnvString);
                        if (dagmanEnvString.isEmpty()) {
                            dagmanEnv.append("environment=");
                        } else {
                            dagmanEnv.append(";");
                        }
                        for (Iterator it = env.getProfileKeyIterator(); it.hasNext(); ) {
                            String key = (String) it.next();
                            dagmanEnv.append(key).append("=").append(env.get(key)).append(";");
                        }
                        mLogger.log(
                                "Updated environment for dagman is " + dagmanEnv.toString(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        raf.writeBytes(dagmanEnv.toString());
                        raf.writeBytes(System.getProperty("line.separator", "\r\n"));
                        raf.writeBytes("queue");
                        break;
                    }
                    previous = raf.getFilePointer();
                }

                raf.close();
            } catch (IOException e) {
                throw new CodeGeneratorException(
                        "Error while reading dagman .condor.sub file " + file, e);
            }
        }
        return true;
    }

    /**
     * Returns the arguments that need to be passed to condor_submit_dag to generate the .condor.sub
     * file corresponding to the dag file
     *
     * @param dag the dag
     * @param dagFile the condor .dag file
     * @return arguments
     * @throws CodeGeneratorException
     */
    protected String getCondorSubmitDagArgs(ADag dag, File dagFile) throws CodeGeneratorException {
        /*
                push( @arg, '-MaxPre', $maxpre ) if $maxpre > 0;
        push( @arg, '-MaxPost', $maxpost ) if $maxpost > 0;
        push( @arg, '-maxjobs', $maxjobs ) if $maxjobs > 0;
        push( @arg, '-maxidle', $maxidle ) if $maxidle > 0;
        push( @arg, '-notification', $notify );
        push( @arg, '-verbose' ) if $verbose;
        push( @arg, '-append', 'executable='.$dagman ) if $dagman;
        push( @arg, '-append', '+pegasus_wf_uuid="'.$config{'wf_uuid'}.'"' );
        push( @arg, '-append', '+pegasus_root_wf_uuid="'.$config{'root_wf_uuid'}.'"' );
        push( @arg, '-append', '+pegasus_wf_name="'.$config{'pegasus_wf_name'}.'"' );
        push( @arg, '-append', '+pegasus_wf_time="'.$config{timestamp}.'"' );
        push( @arg, '-append', '+pegasus_version="'.$config{'planner_version'}.'"' );
        push( @arg, '-append', '+pegasus_job_class=11' );
        push( @arg, '-append', '+pegasus_cluster_size=1' );
        push( @arg, '-append', '+pegasus_site="local"' );
        push( @arg, '-append', '+pegasus_wf_xformation="pegasus::dagman"' );
                */
        StringBuilder args = new StringBuilder();

        // append the executable path to pegasus-dagman
        File pegasusDAGMan = FindExecutable.findExec("pegasus-dagman");
        if (pegasusDAGMan == null) {
            throw new CodeGeneratorException("Unable to determine path to pegasus-dagman");
        }

        args.append("-append ")
                .append("executable")
                .append("=")
                .append(pegasusDAGMan.getAbsolutePath())
                .append(" ");

        StringBuilder sb = new StringBuilder();
        for (Iterator it = mPOptions.getExecutionSites().iterator(); it.hasNext(); ) {
            String site = (String) it.next();
            sb.append(site);
            sb.append(",");
        }
        String execSites = sb.length() > 1 ? sb.substring(0, sb.length() - 1) : sb.toString();

        Map<String, Object> entries = new LinkedHashMap();
        // the root workflow and workflow uuid
        entries.put(ClassADSGenerator.WF_UUID_KEY, dag.getWorkflowUUID());
        entries.put(ClassADSGenerator.ROOT_WF_UUID_KEY, dag.getRootWorkflowUUID());
        entries.put(ClassADSGenerator.WF_NAME_AD_KEY, dag.getFlowName());
        // the workflow time
        if (dag.getMTime() != null) {
            entries.put(ClassADSGenerator.WF_TIME_AD_KEY, dag.getFlowTimestamp());
        }
        entries.put(ClassADSGenerator.VERSION_AD_KEY, dag.getReleaseVersion());

        // update entries with some hardcode pegasus dagman specific ones
        entries.put("pegasus_job_class", 11);
        entries.put("pegasus_cluster_size", 1);
        entries.put("pegasus_site", "local");
        entries.put("pegasus_execution_sites", execSites);
        entries.put("pegasus_wf_xformation", "pegasus::dagman");

        // we do a -no_submit option
        args.append("-no_submit ");

        // PM-949 we cannot have the force option as then the rescue
        // dags for the dag jobs are not submitted when the top level
        // rescue dag is submitted.
        // args.append( "-force " );

        // get any dagman runtime parameters that are controlled
        // via profiles
        args.append(constructDAGManKnobs());

        // construct all the braindump entries as append options to dagman
        for (Map.Entry<String, Object> entry : entries.entrySet()) {
            String key = entry.getKey();
            args.append("-append ").append("+").append(key);
            Object value = entry.getValue();
            if (value instanceof String) {
                args.append("=\"").append(entry.getValue()).append("\"");
            } else {
                args.append("=").append(entry.getValue());
            }
            args.append(" ");
        }

        // the last argument is the path to the .dag file
        args.append(dagFile.getName());

        return args.toString();
    }

    /**
     * Constructs Any extra arguments that need to be passed to dagman, as determined from the
     * properties file.
     *
     * @return any arguments to be added, else empty string
     */
    public String constructDAGManKnobs() {
        StringBuilder sb = new StringBuilder();

        // get all the values for the dagman knows
        int value;
        Properties props = mProps.matchingSubset("dagman", false);
        for (int i = 0; i < DAGMAN_KNOBS.length; i++) {
            String key = DAGMAN_KNOBS[i][0];
            value = parseInt((String) props.get(key));
            if (value < 0 && key.equalsIgnoreCase(Dagman.MAXPOST_KEY)) {
                value = DEFAULT_MAX_POST_SCRIPTS;
            }
            if (value > 0) {
                // add the option
                sb.append(DAGMAN_KNOBS[i][1]);
                sb.append(value);
            }
        }
        sb.append(" ");
        return sb.toString();
    }

    /**
     * Parses a string into an integer. Non valid values returned as -1
     *
     * @param s the String to be parsed as integer
     * @return the int value if valid, else -1
     */
    protected static int parseInt(String s) {
        int value = -1;
        try {
            value = Integer.parseInt(s);
        } catch (Exception e) {
            // ignore
        }
        return value;
    }

    private static class PSDStreamGobblerCallback implements StreamGobblerCallback {

        public static final String[] IGNORE_LOG_LINES = {
            "Renaming rescue DAGs newer than number 0",
            "-no_submit given, not submitting DAG to Condor.",
            "\"condor_submit",
            "I am: hostname:"
        };

        /** */
        private int mLevel;

        /** The instance to the logger to log messages. */
        private LogManager mLogger;

        /**
         * The overloaded constructor.
         *
         * @param level the level on which to log.
         */
        public PSDStreamGobblerCallback(int level) {
            // should do a sanity check on the levels
            mLevel = level;
            mLogger = LogManagerFactory.loadSingletonInstance();
        }

        /**
         * Callback whenever a line is read from the stream by the StreamGobbler. The line is logged
         * to the level specified while initializing the class.
         *
         * @param line the line that is read.
         */
        public void work(String line) {
            boolean log = true;
            for (String ignore : IGNORE_LOG_LINES) {
                if (line.startsWith(ignore)) {
                    log = false;
                    break;
                }
            }

            if (log) {
                mLogger.log(line, mLevel);
            }
        }
    }
}
