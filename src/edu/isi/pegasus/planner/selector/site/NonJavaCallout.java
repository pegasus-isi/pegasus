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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This is the class that implements a call-out to a site selector which is an application or
 * executable script. In order to use the site selector implemented by this class, the property
 * <code>pegasus.selector.site</code> must be set to value <code>NonJavaCallout</code>.
 *
 * <p>This site selector implements a <code>popen()</code> like call to an external application that
 * conforms to the API laid out here. The name of the application to run is specified by the
 * property <code>pegasus.selector.site.path</code>. Its value points to a locally available
 * application.
 *
 * <p>If the external executable requires certain environment variables to be set for execution,
 * these can be specified in the property files, using the prefix <code>pegasus.selector.site.env
 * </code>. The name of the environment variable is obtained by stripping the prefix. For example,
 * to set the variable PATH to a certain value, use the following entry in your user property file:
 *
 * <p>
 *
 * <pre>
 *   pegasus.selector.site.env.PATH = /usr/bin:/bin:/usr/X11R6/bin
 * </pre>
 *
 * The site selector populates the environment of the external application with the following
 * default properties, which can be overwritten by user-specified properties:
 *
 * <p>
 *
 * <table border="1">
 * <tr align="left"><th>key</th><th>value</th></tr>
 * <tr align="left"><th>PEGASUS_HOME</th>
 *  <td>As set by the system</td></tr>
 * <tr align="left"><th>CLASSPATH</th>
 *  <td>From <tt>java.class.path</tt></td></tr>
 * <tr align="left"><th>JAVA_HOME</th>
 *  <td>From <tt>java.home</tt></td></tr>
 * <tr align="left"><th>USER</th>
 *  <td>From <tt>user.name</tt>, if present</td></tr>
 * <tr align="left"><th>LOGNAME</th>
 *  <td>From <tt>user.name</tt>, if present</td></tr>
 * <tr align="left"><th>HOME</th>
 *  <td>From <tt>user.home</tt>, if present</td></tr>
 * <tr align="left"><th>TMP</th>
 *  <td>From <tt>java.io.tmpdir</tt>, if present</td></tr>
 * <tr align="left"><th>TZ</th>
 *  <td>From <tt>user.timezone</tt>, if present</td></tr>
 * </table>
 *
 * <p>The user can specify the environment variables, by specifying the properties with the prefix
 * pegasus.selector.site.env. prefix. for e.g user can override the default user.name property by
 * setting the property pegasus.selector.site.env.user.home .
 *
 * <p>The external application is invoked with one commandline argument. This argument is the name
 * of a temporary file. The temporary file is created for each invocation anew by the site selecting
 * caller. Being temporary, the file is deleted after the site selector returns with success. The
 * deletion of the file is governed by the property pegasus.selector.site.keep.tmp. It can have a
 * tristate value with the valid values being
 *
 * <pre>
 *              ALWAYS
 *              NEVER
 *              ONERROR
 * </pre>
 *
 * <p>The external application is expected to write one line to stdout. The line starts with the
 * string <code>SOLUTION:</code>, followed by the chosen site handle. Optionally, separated by a
 * colon, the name of a jobmanager for the site can be provided by the site selector. Two examples
 * for successful site selections are:
 *
 * <p>
 *
 * <pre>
 *   SOLUTION:mysite:my.job.mgr/jobmanager-batch
 *   SOLUTION:siteY
 * </pre>
 *
 * Note, these are two examples. The site selector only returns one line with the appropriate
 * solution. If no site is found to be eligble, the poolhandle should be set to NONE by the site
 * selector.
 *
 * <p>The temporary file is the corner stone of the communication between the site selecting caller
 * and the external site selector. It is a collection of key-value pairs. Each pair is separated by
 * an equals (=) sign, and stands on a line of its own. There are no multi-line values permitted.
 *
 * <p>The following pairs are generated for the siteselector temporary file:
 *
 * <p>
 *
 * <table border="1">
 * <tr align="left"><th>#</th><th>key</th><th>value</th></tr>
 * <tr align="left"><th>1</th><th>version</th>
 *  <td>The version of the site selector API, currently 2.0</td></tr>
 * <tr align="left"><th>1</th><th>transformation</th>
 *  <td>The fully-qualified definition identifier for the TR, ns::id:vs.</td></tr>
 * <tr align="left"><th>1</th><th>derivation</th>
 *  <td>The fully-qualified definition identifier for the DV, ns::id:vs.</td></tr>
 * <tr align="left"><th>1</th><th>job.level</th>
 *  <td>The job's depth in the DFS tree of the workflow DAG</td></tr>
 * <tr align="left"><th>1</th><th>job.id</th>
 *  <td>The job's ID, as used in the DAX file.</td></tr>
 * <tr align="left"><th>N</th><th>resource.id</th>
 *  <td>A pool handle, followed by a whitespace, followed by a gridftp server.
 *  Typically, each gridftp server is enumerated once, so you may have multiple
 *  occurances of the same site.</td></tr>
 * <tr align="left"><th>M</th><th>input.lfn</th>
 *  <td>An input LFN, optionally followed by a whitespace and filesize.</td></tr>
 * <tr align="left"><th>1</th><th>wf.name</th>
 *  <td>The label of the DAX, as found in the DAX's root element.</td></tr>
 * <tr align="left"><th>1</th><th>wf.index</th>
 *  <td>The DAX index, which is incremented for each partition.</td></tr>
 * <tr align="left"><th>1</th><th>wf.time</th>
 *  <td>The <i>mtime</i> of the workflow.</td></tr>
 * <tr align="left"><th>1</th><th>wf.manager</th>
 *  <td>The name of the workflow manager to be used, e.g. <tt>dagman</tt>.</td></tr>
 * <tr align="left"><th>1</th><th>vo.name</th>
 *  <td>unused at present, name of the virtual organization who runs this WF.</td></tr>
 * <tr align="left"><th>1</th><th>vo.group</th>
 *  <td>unused at present, usage not clear .</td></tr>
 * </table>
 *
 * <p>In order to detect malfunctioning site selectors, a timeout is attached with each site
 * selector, see property <code>pegasus.selector.site.timeout</code>. By default, a site selector is
 * given up upon after 60 s.
 *
 * <p>
 *
 * @author Karan Vahi
 * @author Jens Vöckler
 * @version $Revision$
 * @see java.lang.Runtime
 * @see java.lang.Process
 */
public class NonJavaCallout extends AbstractPerJob {

    /**
     * The prefix to be used while creating a temporary file to pass to the external siteselector.
     */
    public static final String PREFIX_TEMPORARY_FILE = "pegasus";

    /**
     * The suffix to be used while creating a temporary file to pass to the external siteselector.
     */
    public static final String SUFFIX_TEMPORARY_FILE = null;

    /**
     * The prefix of the property names that specify the environment variables that need to be set
     * before calling out to the site selector.
     */
    public static final String PREFIX_PROPERTIES = "pegasus.selector.site.env.";

    /**
     * The prefix that the site selector writes out on its stdout to designate that it is sending a
     * solution.
     */
    public static final String SOLUTION_PREFIX = "SOLUTION:";

    /** The version number associated with this API of non java callout site selection. */
    public static final String VERSION = "2.0";

    // tristate variables for keeping the temporary files generated

    /** The state denoting never to keep the temporary files. */
    public static final int KEEP_NEVER = 0;

    /** The state denoting to keep the temporary files only in case of error. */
    public static final int KEEP_ONERROR = 1;

    /** The state denoting always to keep the temporary files. */
    public static final int KEEP_ALWAYS = 2;

    /** The description of the site selector. */
    private static final String mDescription = "External call-out to a site-selector application";

    /**
     * The map that contains the environment variables including the default ones that are set while
     * calling out to the site selector unless they are overridden by the values set in the
     * properties file.
     */
    private Map mEnvVar;

    /**
     * The timeout value in seconds after which to timeout, in the case where the external site
     * selector does nothing (nothing on stdout nor stderr).
     */
    private int mTimeout;

    /** The tristate value for whether keeping the temporary files generated or not. */
    private int mKeepTMP;

    /** The path to the site selector. */
    private String mSiteSelectorPath;

    /** The abstract DAG. */
    private ADag mAbstractDag;

    /** The default constructor. */
    public NonJavaCallout() {
        super();
        // set the default timeout to 60 seconds
        mTimeout = 60;
        // default would be onerror
        mKeepTMP = KEEP_ONERROR;
    }

    /**
     * Initializes the site selector.
     *
     * @param bag the bag of objects that is useful for initialization.
     */
    public void initialize(PegasusBag bag) {
        super.initialize(bag);
        mTimeout = mProps.getSiteSelectorTimeout();
        mSiteSelectorPath = mProps.getSiteSelectorPath();

        // load the environment variables from the properties file
        // and the default values.
        this.loadEnvironmentVariables();
        // get the value from the properties file.
        mKeepTMP = getKeepTMPValue(mProps.getSiteSelectorKeep());
    }

    /**
     * Maps the jobs in the workflow to the various grid sites. The jobs are mapped by setting the
     * site handle for the jobs.
     *
     * @param workflow the workflow.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public void mapWorkflow(ADag workflow, List sites) {
        mAbstractDag = workflow;
        // PM-747 no need for conversion as ADag now implements Graph interface
        super.mapWorkflow(workflow, sites);
    }

    /**
     * Returns a brief description of the site selection technique implemented by this class.
     *
     * @return a self-description of this site selector.
     */
    public String description() {
        return mDescription;
    }

    /**
     * Calls out to the external site selector. The method converts a <code>Job</code> object into
     * an API-compliant temporary file. The file's name is provided as single commandline argument
     * to the site selector executable when it is invoked. The executable, representing the external
     * site selector, provides its answer on <i>stdout</i>. The answer is captures, and returned.
     *
     * @param job is a representation of the DAX compute job whose site of execution need to be
     *     determined.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     *     <p>FIXME: Some site selector return an empty string on failures. Also: NONE could be a
     *     valid site name.
     * @see org.griphyn.cPlanner.classes.Job
     */
    public void mapJob(Job job, List sites) {
        Runtime rt = Runtime.getRuntime();

        // prepare the temporary file that needs to be sent to the
        // Site Selector via command line.
        File ipFile = prepareInputFile(job, sites);

        // sanity check
        if (ipFile == null) {
            job.setSiteHandle(null);
            return;
        }

        // prepare the environment to call out the site selector
        String command = this.mSiteSelectorPath;
        if (command == null) {
            // delete the temporary file generated
            ipFile.delete();
            throw new RuntimeException(
                    "Site Selector: Please set the path to the external site "
                            + "selector in the properties! ");
        }

        try {
            command += " " + ipFile.getAbsolutePath();

            // get hold of all the environment variables that are to be set
            String[] envArr = this.getEnvArrFromMap();
            mLogger.log("Calling out to site selector " + command, LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec(command, envArr);

            // set up to read subprogram output
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // set up to read subprogram error
            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader ebr = new BufferedReader(err);

            // pipe the process stdout and stderr to standard stdout/stderr
            // FIXME: Really? I thought we want to capture stdout?
            String s = null;
            String se = null;

            // set the variable to check if the timeout needs to be set or not
            boolean notTimeout = (mTimeout <= 0);

            boolean stdout = false;
            boolean stderr = false;
            int time = 0;
            while (((stdout = br.ready()) || (stderr = ebr.ready()))
                    || notTimeout
                    || time < mTimeout) {

                if (!(stdout || stderr)) {
                    // nothing on either streams
                    // sleep for some time
                    try {
                        time += 5;
                        mLogger.log(
                                "main thread going to sleep " + time,
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        Thread.sleep(5000);
                        mLogger.log("main thread woken up", LogManager.DEBUG_MESSAGE_LEVEL);
                    } catch (InterruptedException e) {
                        // do nothing
                        // we potentially loose time here.
                    }
                } else {
                    // we hearing something from selector
                    // reset the time counter
                    time = 0;

                    if (stdout) {
                        s = br.readLine();
                        mLogger.log("[Site Selector stdout] " + s, LogManager.DEBUG_MESSAGE_LEVEL);

                        // parse the string to get the output
                        if (parseStdOut(job, s)) {
                            break;
                        }
                    }

                    if (stderr) {
                        se = ebr.readLine();
                        mLogger.log("[Site Selector stderr] " + se, LogManager.ERROR_MESSAGE_LEVEL);
                    }
                }
            } // while

            // close the streams
            br.close();
            ebr.close();

            if (time >= mTimeout) {
                mLogger.log(
                        "External Site Selector timeout after " + mTimeout + " seconds",
                        LogManager.ERROR_MESSAGE_LEVEL);
                p.destroy();
                // no use closing the streams as it would be probably hung
                job.setSiteHandle(null);
                return;
            }

            // the site selector seems to have worked without any errors
            // delete the temporary file that was generated only if the
            // process exited with a status of 0
            // FIXME: Who is going to clean up after us?
            int status = p.waitFor();
            if (status != 0) {
                // let the user know site selector exited with non zero
                mLogger.log(
                        "Site Selector exited with non zero exit " + "status " + status,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
            // delete the temporary file on basis of keep value
            if ((status == 0 && mKeepTMP < KEEP_ALWAYS)
                    || (status != 0 && mKeepTMP == KEEP_NEVER)) {
                // deleting the file
                if (!ipFile.delete())
                    mLogger.log(
                            "Unable to delete temporary file " + ipFile.getAbsolutePath(),
                            LogManager.WARNING_MESSAGE_LEVEL);
            }

        } catch (IOException e) {
            mLogger.log("[Site selector] " + e.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        } catch (InterruptedException e) {
            mLogger.log(
                    "Waiting for site selector to exit: " + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
        }

        return;
    }

    /**
     * Writes job knowledge into the temporary file passed to the external site selector. The job
     * knowledge derives from the contents of the DAX job's <code>Job</code> record, and the a list
     * of site candidates. The format of the file is laid out in the class's introductory
     * documentation.
     *
     * @param job is a representation of the DAX compute job whose site of execution need to be
     *     determined.
     * @param pools is a list of site candidates. The items of the list are <code>String</code>
     *     objects.
     * @return the temporary input file was successfully prepared. A value of <code>null</code>
     *     implies that an error occured while writing the file.
     * @see #getTempFilename()
     */
    private File prepareInputFile(Job job, List pools) {
        File f = new File(this.getTempFilename());
        PrintWriter pw;

        try {
            pw = new PrintWriter(new FileWriter(f));

            // write out the version of the api
            pw.println("version=" + this.VERSION);

            // fw.write("\nvds_job_name=" + job.jobName);
            pw.println("transformation=" + job.getCompleteTCName());
            pw.println("derivation=" + job.getCompleteDVName());

            // write out the job id and level as gotten from dax
            pw.println("job.level=" + job.level);
            pw.println("job.id=" + job.logicalId);

            // at present Pegasus always asks to schedule compute jobs
            // User should be able to specify through vdl or the pool config file.
            // Karan Feb 10 3:00 PM PDT
            // pw.println("vds_scheduler_preference=regular");

            // write down the list of exec Pools and their corresponding grid
            // ftp servers
            if (pools.isEmpty()) {
                // just write out saying illustrating no exec pool or grid ftp
                // server passed to site selector. Upto the selector to do what
                // it wants.

                // FIXME: We need to define this part of the interface. If there
                // are not site candidates, should it ever reach this part of
                // the code? If now, insert assertion and abort here. If yes, we
                // need to define this case! But just silently write the below
                // will not site will with our set of site selectors.
                pw.println("resource.id=NONE NONE");
            } else {
                String st, pool;
                for (Iterator i = pools.iterator(); i.hasNext(); ) {
                    pool = (String) i.next();
                    st = "resource.id=" + pool + " ";

                    SiteCatalogEntry site = mSiteStore.lookup(pool);
                    /*
                    for( Iterator it = site.getHeadNodeFS().getScratch().getSharedDirectory().getFileServersIterator(); it.hasNext();){
                        pw.println(st + ( (FileServer) it.next()).getURLPrefix() );
                    }*/
                    Directory d = site.getDirectory(Directory.TYPE.shared_scratch);
                    if (d != null) {
                        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
                            for (Iterator it = d.getFileServersIterator(op); it.hasNext(); ) {
                                pw.println(st + ((FileServer) it.next()).getURLPrefix());
                            }
                        }
                    }
                } // for
            }

            // write the input files
            for (Iterator i = job.inputFiles.iterator(); i.hasNext(); )
                pw.println("input.lfn=" + ((PegasusFile) i.next()).getLFN());

            // write workflow related metadata
            if (this.mAbstractDag != null) {
                pw.println("wf.name=" + mAbstractDag.getLabel());
                pw.println("wf.index=" + mAbstractDag.getIndex());
                // pw.println("workflow.time=" + mAbstractDag.dagInfo.time??);
                // FIXME: Try File.lastModified() on the DAX file

                // should actually be picked up from the properties file
                pw.println("wf.manager=" + "dagman");
            }

            // uninitialized values
            pw.println("vo.name=" + "NONE");
            pw.println("vo.group=" + "NONE");

            // done
            pw.flush();
            pw.close();

        } catch (IOException e) {
            mLogger.log(
                    "While writing to the temporary file :" + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return null;

        } catch (Exception ex) {
            // an unknown exception
            mLogger.log(
                    "Unknown error while writing to the temp file :" + ex.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        return f;
    }

    /**
     * Extracts the chosen site from the site selector's answer. Parses the <i>stdout</i> sent by
     * the selector, to see, if the execution pool and the jobmanager were sent or not.
     *
     * @param job the job that has to be mapped.
     * @param s is the stdout received from the site selector.
     * @return boolean indicating if the stdout was succesfully parsed and job populated.
     */
    private boolean parseStdOut(Job job, String s) {
        String val = null;

        s = s.trim();
        boolean result = false;
        if (s.startsWith(SOLUTION_PREFIX)) {
            s = s.substring(SOLUTION_PREFIX.length());

            StringTokenizer st = new StringTokenizer(s, ":");

            while (st.hasMoreTokens()) {
                result = true;
                job.setSiteHandle((String) st.nextToken());

                job.setJobManager(st.hasMoreTokens() ? st.nextToken() : null);
            }
        }

        // HMMM: String.indexOf() functions can be used in Jens HO.
        return result;
    }

    /**
     * Creates a temporary file and obtains its name. This method returns the absolute path to a
     * temporary file in the system's TEMP directory. The file is guarenteed to be unique for the
     * current invocation of the virtual machine.
     *
     * <p>FIXME: However, since we return a filename and not an opened file, race conditions are
     * still possible.
     *
     * @return the absolute path of a newly created temporary file.
     */
    private String getTempFilename() {
        File f = null;
        try {
            f = File.createTempFile(PREFIX_TEMPORARY_FILE, SUFFIX_TEMPORARY_FILE);
            return f.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Unable to get handle to a temporary file :" + e.getMessage());
        }
    }

    /**
     * Initializes the internal hash that collects environment variables. These variables are set up
     * to run the external helper application. Environment variables come from two source.
     *
     * <ol>
     *   <li>Default environment variables, fixed, hard-coded.
     *   <li>User environment variables, from properties.
     * </ol>
     */
    private void loadEnvironmentVariables() {
        // load the default environment variables
        String value = null;
        mEnvVar = new HashMap();
        mEnvVar.put("CLASSPATH", mProps.getProperty("java.class.path"));
        mEnvVar.put("JAVA_HOME", mProps.getProperty("java.home"));

        // set $LOGNAME and $USER if corresponding property set in JVM
        if ((value = mProps.getProperty("user.name")) != null) {
            mEnvVar.put("USER", value);
            mEnvVar.put("LOGNAME", value);
        }

        // set the $HOME if user.home is set
        if ((value = mProps.getProperty("user.home")) != null) mEnvVar.put("HOME", value);

        // set the $TMP if java.io.tmpdir is set
        if ((value = mProps.getProperty("java.io.tmpdir")) != null) mEnvVar.put("TMP", value);

        // set $TZ if user.timezone is set
        if ((value = mProps.getProperty("user.timezone")) != null) mEnvVar.put("TZ", value);

        // get hold of the environment variables that user might have set
        // and put them in the map overriding the variables already set.
        mEnvVar.putAll(mProps.matchingSubset(PREFIX_PROPERTIES, false));
    }

    /**
     * Generates an array of environment variables. The variables are kept in an internal map.
     * Converts the environment variables in the map to the array format.
     *
     * @return array of enviroment variables set, or <code>null</code> if the map is empty.
     * @see #loadEnvironmentVariables()
     */
    private String[] getEnvArrFromMap() {
        String result[] = null;

        // short-cut
        if (mEnvVar == null || mEnvVar.isEmpty()) return result;
        else result = new String[mEnvVar.size()];

        Iterator it = mEnvVar.entrySet().iterator();
        int i = 0;
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            result[i] = entry.getKey() + "=" + entry.getValue();
            i++;
        }

        return result;
    }

    /**
     * Returns the int value corresponding to the string value passed.
     *
     * @param value the string value for keeping the temporary files.
     * @return the corresponding int value.
     * @see #KEEP_ALWAYS
     * @see #KEEP_NEVER
     * @see #KEEP_ONERROR
     */
    private int getKeepTMPValue(String value) {
        // default value is keep on error
        int val = KEEP_ONERROR;

        // sanity check of the string value
        if (value == null || value.length() == 0) {
            // return the default value
            return val;
        }
        value = value.trim();
        if (value.equalsIgnoreCase("always")) val = KEEP_ALWAYS;
        if (value.equalsIgnoreCase("never")) val = KEEP_NEVER;

        return val;
    }

    /**
     * The main program that allows you to test. FIXME: Test programs should have prefix
     * Test.....java
     *
     * @param args the arguments
     */
    public static void main(String[] args) {
        LogManagerFactory.loadSingletonInstance().setLevel(LogManager.DEBUG_MESSAGE_LEVEL);

        NonJavaCallout nj = new NonJavaCallout();

        Job s = new Job();
        s.logicalName = "test";
        s.namespace = "pegasus";
        s.version = "1.01";
        s.jobName = "test_ID00001";

        List pools = new java.util.ArrayList();
        pools.add("isi-condor");
        pools.add("isi-lsf");

        nj.mapJob(s, pools);
        System.out.println("Exec Pool return by site selector is " + s.getSiteHandle());
    }
}
