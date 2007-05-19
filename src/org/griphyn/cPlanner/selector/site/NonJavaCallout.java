/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.selector.site;

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

import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;


import org.griphyn.cPlanner.selector.SiteSelector;

/**
 * This is the class that implements a call-out to a site selector which
 * is an application or executable script. In order to use the site
 * selector implemented by this class, the property
 * <code>pegasus.selector.site</code> must be set to value
 * <code>NonJavaCallout</code>.<p>
 *
 * This site selector implements a <code>popen()</code> like call to an
 * external application that conforms to the API laid out here. The name
 * of the application to run is specified by the property
 * <code>pegasus.selector.site.path</code>. Its value points to a locally
 * available application.<p>
 *
 * If the external executable requires certain environment variables to
 * be set for execution, these can be specified in the property files,
 * using the prefix <code>pegasus.selector.site.env</code>. The name of the
 * environment variable is obtained by stripping the prefix. For
 * example, to set the variable PATH to a certain value, use the
 * following entry in your user property file:<p>
 *
 * <pre>
 *   pegasus.selector.site.env.PATH = /usr/bin:/bin:/usr/X11R6/bin
 * </pre>
 *
 * The site selector populates the environment of the external
 * application with the following default properties, which can
 * be overwritten by user-specified properties:<p>
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
 * </table><p>
 *
 * The user can specify the environment variables, by specifying the
 * properties with the prefix pegasus.selector.site.env. prefix. for e.g user
 * can override the default user.name property by setting the property
 * pegasus.selector.site.env.user.home .<p>
 *
 * The external application is invoked with one commandline argument.
 * This argument is the name of a temporary file. The temporary file is
 * created for each invocation anew by the site selecting caller. Being
 * temporary, the file is deleted after the site selector returns with
 * success. The deletion of the file is governed by the property
 * pegasus.selector.site.keep.tmp. It can have a tristate value with the valid
 * values being
 * <pre>
 *              ALWAYS
 *              NEVER
 *              ONERROR
 * </pre>
 * <p>
 *
 * The external application is expected to write one line to stdout.
 * The line starts with the string <code>SOLUTION:</code>, followed
 * by the chosen site handle. Optionally, separated by a colon, the
 * name of a jobmanager for the site can be provided by the site
 * selector. Two examples for successful site selections are:<p>
 *
 * <pre>
 *   SOLUTION:mysite:my.job.mgr/jobmanager-batch
 *   SOLUTION:siteY
 * </pre>
 *
 * Note, these are two examples. The site selector only returns one line
 * with the appropriate solution. If no site is found to be eligble, the
 * poolhandle should be set to NONE by the site selector. <p>
 *
 * The temporary file is the corner stone of the communication between
 * the site selecting caller and the external site selector. It is a
 * collection of key-value pairs. Each pair is separated by an equals
 * (=) sign, and stands on a line of its own. There are no multi-line
 * values permitted.<p>
 *
 * The following pairs are generated for the siteselector temporary file:<p>
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
 * </table><p>
 *
 * In order to detect malfunctioning site selectors, a timeout is
 * attached with each site selector, see property
 * <code>pegasus.selector.site.timeout</code>. By default, a site selector
 * is given up upon after 60 s.<p>
 *
 * @author Karan Vahi
 * @author Jens Vöckler
 *
 * @version $Revision$
 *
 * @see java.lang.Runtime
 * @see java.lang.Process
 */
public class NonJavaCallout extends SiteSelector {

    /**
     * The prefix to be used while creating a temporary file to pass to
     * the external siteselector.
     */
    public static final String PREFIX_TEMPORARY_FILE = "pegasus";

    /**
     * The suffix to be used while creating a temporary file to pass to
     * the external siteselector.
     */
    public static final String SUFFIX_TEMPORARY_FILE = null;


    /**
     * The prefix of the property names that specify the environment
     * variables that need to be set before calling out to the site
     * selector.
     */
    public static final String PREFIX_PROPERTIES = "pegasus.selector.site.env.";

    /**
     * The prefix that the site selector writes out on its stdout to
     * designate that it is sending a solution.
     */
    public static final String SOLUTION_PREFIX = "SOLUTION:";

    /**
     * The version number associated with this API of non java callout
     * site selection.
     */
    public static final String VERSION = "2.0";

    //tristate variables for keeping the temporary files generated

    /**
     * The state denoting never to keep the temporary files.
     */
    public static final int KEEP_NEVER = 0;

    /**
     * The state denoting to keep the temporary files only in case of error.
     */
    public static final int KEEP_ONERROR = 1;

    /**
     * The state denoting always to keep the temporary files.
     */
    public static final int KEEP_ALWAYS = 2;

    /**
     * The description of the site selector.
     */
    private static final String mDescription =
        "External call-out to a site-selector application";

    /**
     * The handle to the internal logger, that Pegasus uses to log all the
     * stdout and stderr it generates.
     */
    private LogManager mLogger;

    /**
     * The string holding the logging messages.
     */
    private String mLogMsg;

    /**
     * The map that contains the environment variables including the
     * default ones that are set while calling out to the site selector
     * unless they are overridden by the values set in the properties
     * file.
     */
    private Map mEnvVar;

    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the pool configuration files.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * The timeout value in seconds after which to timeout, in the case
     * where the external site selector does nothing (nothing on stdout
     * nor stderr).
     */
    private int mTimeout;

    /**
     * The tristate value for whether keeping the temporary files generated or
     * not.
     */
    private int mKeepTMP;

    /**
     * The default constructor.
     */
    public NonJavaCallout(){
        super();
        mLogger = LogManager.getInstance();
        mLogMsg = new String();
        mPoolHandle = null;
        mEnvVar = null;
        // set the default timeout to 60 seconds
        mTimeout = 60;
        //default would be onerror
        mKeepTMP = KEEP_ONERROR;
    }

    /**
     * Initializes a site selector to call an external application.
     *
     * @param path the path to the executable that invokes the SiteSelector on
     *             the command line.
     */
    public NonJavaCallout(String path){
        super(path);

        mLogger = LogManager.getInstance();
        mLogMsg = new String();
        mProps   = PegasusProperties.nonSingletonInstance();
        mTimeout = mProps.getSiteSelectorTimeout();

        String poolClass = PoolMode.getImplementingClass(mProps.getPoolMode());
        mPoolHandle      = PoolMode.loadPoolInstance(poolClass, mProps.getPoolFile(),
            PoolMode.SINGLETON_LOAD);
        // load the environment variables from the properties file
        // and the default values.
        this.loadEnvironmentVariables();
        //get the value from the properties file.
        mKeepTMP = getKeepTMPValue(mProps.getSiteSelectorKeep());
    }

    /**
     * Returns a brief description of the site selection technique
     * implemented by this class.
     *
     * @return a self-description of this site selector.
     */
    public String description(){
        return mDescription;
    }

    /**
     * Calls out to the external site selector. The method converts a
     * <code>SubInfo</code> object into an API-compliant temporary file.
     * The file's name is provided as single commandline argument to the
     * site selector executable when it is invoked. The executable,
     * representing the external site selector, provides its answer
     * on <i>stdout</i>. The answer is captures, and returned.
     *
     * @param job is a representation of the DAX compute job whose site of
     * execution need to be determined.
     *
     * @param pools is a list of site candidates. The items of the list are
     * <code>String</code> objects.
     *
     * @return <code>null</code> in case an error with the site selection
     * occurred. Otherwise, a string that is either solely the site handle,
     * or the site handle and a job manager, separated by a colon. If no
     * pool is found, the result will be <code>NONE</code>.
     *
     * FIXME: Some site selector return an empty string on failures. Also:
     * NONE could be a valid site name.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo
     */
    public String mapJob2ExecPool( SubInfo job, List pools ){
        String execPool = null;
        Runtime rt = Runtime.getRuntime();

        // prepare the temporary file that needs to be sent to the
        // Site Selector via command line.
        File ipFile = prepareInputFile(job,pools);

        // sanity check
        if(ipFile == null)
            return null;

        // prepare the environment to call out the site selector
        String command = this.mSiteSelectorPath;
        if ( command == null ) {
            // delete the temporary file generated
            ipFile.delete();
            throw new RuntimeException( "Site Selector: Please set the path to the external site " +
                                        "selector in the properties! " );
        }

        try {
            command += " " + ipFile.getAbsolutePath();

            // get hold of all the environment variables that are to be set
            String[] envArr = this.getEnvArrFromMap();
            mLogger.log("Calling out to site selector " + command,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec(command , envArr);

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
            boolean notTimeout = ( mTimeout <= 0 );

            boolean stdout = false;
            boolean stderr = false;
            int time = 0;
            while( ( (stdout =br.ready()) || (stderr = ebr.ready()) ) ||
                   notTimeout ||
                   time < mTimeout){

                if ( ! ( stdout || stderr ) ) {
                    // nothing on either streams
                    // sleep for some time
                    try {
                        time +=5;
                        mLogger.log("main thread going to sleep " + time,
                                    LogManager.DEBUG_MESSAGE_LEVEL);
                        Thread.sleep(5000);
                        mLogger.log("main thread woken up",
                                    LogManager.DEBUG_MESSAGE_LEVEL);
                    } catch ( InterruptedException e ) {
                        // do nothing
                        // we potentially loose time here.
                    }
                } else {
                    // we hearing something from selector
                    // reset the time counter
                    time = 0;

                    if ( stdout ) {
                        s = br.readLine();
                        mLogger.log("[Site Selector stdout] " + s,
                                    LogManager.DEBUG_MESSAGE_LEVEL);

                        // parse the string to get the output
                        execPool = parseStdOut(s);
                        if(execPool != null)
                            break;

                    }

                    if ( stderr ) {
                        se = ebr.readLine();
                        mLogger.log("[Site Selector stderr] " + se,
                                    LogManager.ERROR_MESSAGE_LEVEL);
                    }
                }
            } // while

            // close the streams
            br.close();
            ebr.close();

            if ( time >= mTimeout ) {
                mLogger.log("External Site Selector timeout after " +
                            mTimeout + " seconds", LogManager.ERROR_MESSAGE_LEVEL);
                p.destroy();
                // no use closing the streams as it would be probably hung
                return null;
            }

            // the site selector seems to have worked without any errors
            // delete the temporary file that was generated only if the
            // process exited with a status of 0
            // FIXME: Who is going to clean up after us?
            int status = p.waitFor();
            if ( status != 0){
                // let the user know site selector exited with non zero
                mLogger.log("Site Selector exited with non zero exit " +
                            "status " + status, LogManager.DEBUG_MESSAGE_LEVEL);
            }
            //delete the temporary file on basis of keep value
            if((status == 0 && mKeepTMP < KEEP_ALWAYS) ||
               (status != 0  && mKeepTMP == KEEP_NEVER )){
                //deleting the file
                if ( ! ipFile.delete() )
                    mLogger.log("Unable to delete temporary file " +
                                ipFile.getAbsolutePath(),LogManager.WARNING_MESSAGE_LEVEL);
            }

        } catch ( IOException e ) {
            mLogger.log("[Site selector] " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        } catch ( InterruptedException e ) {
            mLogger.log("Waiting for site selector to exit: " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return execPool;
    }


    /**
     * Writes job knowledge into the temporary file passed to the external
     * site selector. The job knowledge derives from the contents of the
     * DAX job's <code>SubInfo</code> record, and the a list of site
     * candidates. The format of the file is laid out in the class's
     * introductory documentation.
     *
     * @param job is a representation of the DAX compute job whose site of
     * execution need to be determined.
     *
     * @param pools is a list of site candidates. The items of the list are
     * <code>String</code> objects.
     *
     * @return the temporary input file was successfully prepared. A value
     * of <code>null</code> implies that an error occured while writing
     * the file.
     *
     * @see #getTempFilename()
     */
    private File prepareInputFile( SubInfo job, List pools ) {
        File f = new File( this.getTempFilename() );
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

            //at present Pegasus always asks to schedule compute jobs
            //User should be able to specify through vdl or the pool config file.
            //Karan Feb 10 3:00 PM PDT
            //pw.println("vds_scheduler_preference=regular");

            // write down the list of exec Pools and their corresponding grid
            // ftp servers
            if ( pools.isEmpty() ) {
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
                for ( Iterator i = pools.iterator(); i.hasNext(); ) {
                    pool = (String) i.next();
                    st = "resource.id=" + pool + " ";

                    // get handle to pool config
                    List l = mPoolHandle.getGridFTPServers(pool);
                    if (l == null || l.isEmpty()) {
                        // FIXME: How hard should this error be?
                        mLogger.log("Site " + pool +
                                    " has no grid ftp" +
                                    "servers associated with it",
                                    LogManager.WARNING_MESSAGE_LEVEL);
                        // append a NONE grid ftp server
                        pw.println(st + "NONE");
                    } else {
                        for ( Iterator j=l.iterator(); j.hasNext(); ) {
                            pw.println(st + ( (GridFTPServer) j.next()).
                                       getInfo(GridFTPServer.GRIDFTP_URL));
                        }
                    }
                } // for
            }

            // write the input files
            for ( Iterator i=job.inputFiles.iterator(); i.hasNext(); )
                pw.println("input.lfn=" + ((PegasusFile)i.next()).getLFN());

                // write workflow related metadata
            if ( this.mAbstractDag != null ) {
                pw.println("wf.name=" + mAbstractDag.dagInfo.nameOfADag);
                pw.println("wf.index=" + mAbstractDag.dagInfo.index);
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

        } catch ( IOException e ) {
            mLogger.log("While writing to the temporary file :" + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            return null;

        } catch ( Exception ex ) {
            //an unknown exception
            mLogger.log("Unknown error while writing to the temp file :" +
                        ex.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        return f;
    }


    /**
     * Extracts the chosen site from the site selector's answer. Parses
     * the <i>stdout</i> sent by the selector, to see, if the execution
     * pool and the jobmanager were sent or not.
     *
     * @param s is the stdout received from the site selector.
     * @return if the pool is found, a string of the form
     * <code>executionpool:jobmanager</code>. The portion of colon and
     * jobmanager do not need to exist. In case of an error,
     * <code>null</code> is returned.
     */
    private String parseStdOut(String s){
        String val = null;

        s = s.trim();
        if(s.startsWith(SOLUTION_PREFIX)){
            s = s.substring(SOLUTION_PREFIX.length());

            StringTokenizer st = new StringTokenizer(s,":");

           while(st.hasMoreTokens()){
               val = (String)st.nextToken() + ":";

               val += (st.hasMoreTokens())?
                       st.nextToken():
                       "null";
           }
        }

        // HMMM: String.indexOf() functions can be used in Jens HO.
        return val;
    }

    /**
     * Creates a temporary file and obtains its name. This method returns
     * the absolute path to a temporary file in the system's TEMP
     * directory. The file is guarenteed to be unique for the current
     * invocation of the virtual machine.
     *
     * FIXME: However, since we return a filename and not an opened file, race
     * conditions are still possible.
     *
     * @return the absolute path of a newly created temporary file.
     */
    private String getTempFilename(){
        File f = null;
        try {
            f = File.createTempFile(PREFIX_TEMPORARY_FILE,SUFFIX_TEMPORARY_FILE);
            return f.getAbsolutePath();
        } catch ( IOException e ) {
            throw new RuntimeException( "Unable to get handle to a temporary file :" + e.getMessage());
        }
    }

    /**
     * Initializes the internal hash that collects environment variables.
     * These variables are set up to run the external helper application.
     * Environment variables come from two source.
     *
     * <ol>
     * <li>Default environment variables, fixed, hard-coded.
     * <li>User environment variables, from properties.
     * </ol>
     */
    private void loadEnvironmentVariables(){
        // load the default environment variables
        String value = null;
        mEnvVar = new HashMap();
        mEnvVar.put("PEGASUS_HOME", mProps.getPegasusHome());
        mEnvVar.put("CLASSPATH",mProps.getProperty("java.class.path"));
        mEnvVar.put("JAVA_HOME",mProps.getProperty("java.home"));

        // set $LOGNAME and $USER if corresponding property set in JVM
        if ( (value = mProps.getProperty("user.name")) != null ) {
            mEnvVar.put("USER",value);
            mEnvVar.put("LOGNAME",value);
        }

        // set the $HOME if user.home is set
        if ( (value = mProps.getProperty("user.home")) != null )
            mEnvVar.put("HOME",value);

            // set the $TMP if java.io.tmpdir is set
        if ( (value = mProps.getProperty("java.io.tmpdir")) != null )
            mEnvVar.put("TMP",value);

            // set $TZ if user.timezone is set
        if ( (value = mProps.getProperty("user.timezone")) != null )
            mEnvVar.put("TZ",value);

        // get hold of the environment variables that user might have set
        // and put them in the map overriding the variables already set.
        mEnvVar.putAll( mProps.matchingSubset(PREFIX_PROPERTIES,false) );
  }

  /**
   * Generates an array of environment variables. The variables are kept
   * in an internal map. Converts the environment variables in the map
   * to the array format.
   *
   * @return array of enviroment variables set, or <code>null</code> if
   * the map is empty.
   * @see #loadEnvironmentVariables()
   */
  private String[] getEnvArrFromMap(){
      String result[] = null;

      // short-cut
      if ( mEnvVar == null || mEnvVar.isEmpty() )
          return result;
      else
          result = new String[mEnvVar.size()];

      Iterator it = mEnvVar.entrySet().iterator();
      int i = 0;
      while(it.hasNext()){
          Map.Entry entry = (Map.Entry)it.next();
          result[i] = entry.getKey() + "=" + entry.getValue();
          i++;
      }

      return result;
  }

  /**
   * Returns the int value corresponding to the string value passed.
   *
   * @param value  the string value for keeping the temporary files.
   *
   * @return  the corresponding int value.
   * @see #KEEP_ALWAYS
   * @see #KEEP_NEVER
   * @see #KEEP_ONERROR
   */
  private int getKeepTMPValue(String value){
      //default value is keep on error
      int val = KEEP_ONERROR;

      //sanity check of the string value
      if(value == null || value.length() == 0){
          //return the default value
          return val;
      }
      value = value.trim();
      if(value.equalsIgnoreCase("always"))
          val = KEEP_ALWAYS;
      if(value.equalsIgnoreCase("never"))
          val = KEEP_NEVER;

      return val;
  }

  /**
   * The main program that allows you to test.
   * FIXME: Test programs should have prefix Test.....java
   *
   */
  public static void main( String[] args ){
      LogManager.getInstance().setLevel(LogManager.DEBUG_MESSAGE_LEVEL);

      NonJavaCallout nj = new NonJavaCallout(
            PegasusProperties.nonSingletonInstance().getSiteSelectorPath());

      SubInfo s = new SubInfo();
      s.logicalName = "test";
      s.namespace   = "pegasus";
      s.version     = "1.01";
      s.jobName     = "test_ID00001";

      List pools = new java.util.ArrayList();
      pools.add("isi-condor");pools.add("isi-lsf");

      String execPool = nj.mapJob2ExecPool(s,pools);
      System.out.println("Exec Pool return by site selector is " + execPool);
  }

}
