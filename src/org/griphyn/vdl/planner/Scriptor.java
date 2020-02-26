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

package org.griphyn.vdl.planner;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import java.io.*;
import java.util.*;
import org.griphyn.vdl.classes.LFN;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.util.Logging;

/**
 * This class generates the shell scripts from a DAX. There is a script for each job in the dag, and
 * there is a control script to coordinate these jobs.
 *
 * <p>The scripts are assembled mostly from template files and substitutions. The template files
 * reside in <code>$PEGASUS_HOME/share</code>:
 *
 * <p>
 *
 * <table border="1">
 * <tr><th>template</th><th>purpose</th></tr>
 * <tr><td>sp-job-1.tmpl</td><td>start of job script</td></tr>
 * <tr><td>sp-job-2.tmpl</td><td>unused</td></tr>
 * <tr><td>sp-job-3.tmpl</td><td>final portion of job script</td></tr>
 * <tr><td>sp-master-1.tmpl</td><td>start of master script</td></tr>
 * <tr><td>sp-master-2.tmpl</td><td>intermediary of master script</td></tr>
 * <tr><td>sp-master-3.tmpl</td><td>final portion of master script</td></tr>
 * <tr><td>sp-master-job.tmpl</td><td>Invocation of job from master</td></tr>
 * </table>
 *
 * The following substitutions are available by default. Some substitutions are only available
 * during job generation:
 *
 * <p>
 *
 * <table border="1">
 * <tr><th>variable</th><th>purpose</th></tr>
 * <tr><td>DAXLABEL</td><td>user-given label of the workflow</td></tr>
 * <tr><td>DV</td><td>Job: fully-qualified DV of job</td></tr>
 * <tr><td>FILELIST</td><td>Job: Name of file of output mappings</td></tr>
 * <tr><td>HOME</td><td>JRE system property user.home</td></tr>
 * <tr><td>JOBID</td><td>Job: the IDxxxxx of the current job</td></tr>
 * <tr><td>JOBLOG</td><td>Job: the log file of the job</td></tr>
 * <tr><td>JOBSCRIPT</td><td>Job: name of script file for job</td></tr>
 * <tr><td>KICKSTART</td><td>if set, path to local kickstart</td><tr>
 * <tr><td>LOGFILE</td><td>Name of master log file</td></tr>
 * <tr><td>NOW</td><tr>Start time stamp of processing (compile time)</td></tr>
 * <tr><td>REGISTER</td><td>0 or 1 for replica registration</td></tr>
 * <tr><td>TR</td><td>Job: fully-qualified TR of job</td></tr>
 * <tr><td>USER</td><td>JRE system property user.name</td></tr>
 * </table>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Scriptor {
    /** the directory to put the scripts */
    private String m_dirName;

    /** the dag structure */
    private ADAG m_adag;

    /** name of the dag */
    private String m_dagName;

    /** replica catalog */
    private RCWrapper m_rc;

    /** site catalog (optional) */
    private SCWrapper m_sc;

    /** transformation catalog */
    private TCWrapper m_tc;

    /** the hash that holds all the lfn->pfn mapping */
    private HashMap m_filenameMap;

    /** the name of the master log file. */
    private String m_logFile;

    /** whether to register output files */
    private boolean m_register;

    /** path to kickstart */
    private String m_kickstart;

    /** buffered writer for control script file */
    private BufferedWriter m_master;

    /** Stores the reference to the logger. */
    private Logging m_log;

    /** holds the location where templates reside. */
    private File m_dataDir;

    /** holds the mapping for permissable substitutions. */
    private Map m_substitute = null;

    /** a private copy of this environment's notion of a line separator. */
    private static final String newline = System.getProperty("line.separator", "\r\n");

    /**
     * Constructor
     *
     * @param dirName names the directory into which to produce the scripts.
     * @param adag is the DAX as a parsed data structure in memory.
     * @param rc is the replica catalog wrapper.
     * @param sc is the site catalog wrapper, may be <code>null</code>.
     * @param tc is the transformation catalog wrapper.
     * @param fnMap is a map containing all filesnames in the DAG.
     * @param dataDir is the location of $PEGASUS_HOME/share from properties.
     */
    public Scriptor(
            String dirName,
            ADAG adag,
            RCWrapper rc,
            SCWrapper sc,
            TCWrapper tc,
            HashMap fnMap,
            File dataDir) {
        m_dirName = dirName;
        m_adag = adag;
        m_dataDir = dataDir;

        // set dag name
        m_dagName = adag.getName();
        if (m_dagName == null) m_dagName = m_dirName;

        m_rc = rc;
        m_sc = sc;
        m_tc = tc;
        m_filenameMap = fnMap;

        m_logFile = m_dagName + ".log";
        m_register = true;
        m_kickstart = null;
        if (m_sc != null) {
            String kl = m_sc.getGridLaunch();
            if (kl != null) {
                File k = new File(kl);
                if (k.exists()) m_kickstart = kl;
            }
        }
        m_log = Logging.instance();

        // prepare substitutions
        m_substitute = new TreeMap();
        m_substitute.put("NOW", Currently.iso8601(false, true, false, new Date()));
        m_substitute.put("DAXLABEL", m_dagName);
        m_substitute.put("USER", System.getProperty("user.name"));
        m_substitute.put("HOME", System.getProperty("user.home"));
        m_substitute.put("LOGFILE", m_logFile);
        if (m_kickstart != null) m_substitute.put("KICKSTART", m_kickstart);
        m_substitute.put("REGISTER", m_register ? "1" : "0");
    }

    /**
     * Sets the flag indicating whether to register output files.
     *
     * @param b is a flag to set the registration state.
     * @see #getRegister()
     */
    public void setRegister(boolean b) {
        this.m_register = b;
        addSubstitution("REGISTER", b ? "1" : "0");
    }

    /**
     * Gets the flag indicating whether to register output files.
     *
     * @return true, if output files are going to be registered.
     * @see #setRegister(boolean)
     */
    public boolean getRegister() {
        return this.m_register;
    }

    /**
     * Sets kickstart path, if the path is null, kickstart will not be used.
     *
     * @param kickstart the path to invoke kickstart
     * @see #getKickstart()
     */
    public void setKickstart(String kickstart) {
        m_kickstart = kickstart;
        if (kickstart != null) addSubstitution("KICKSTART", kickstart);
        else removeSubstitution("KICKSTART");
    }

    /**
     * Gets the current kickstart path. The location may be null.
     *
     * @return the path to kickstart, or <code>null</code>
     * @see #setKickstart( String )
     */
    public String getKickstart() {
        return this.m_kickstart;
    }

    /**
     * Inserts a substitution into the substitutable variables.
     *
     * @param key is the template variable name
     * @param value is the replacement
     * @return the previous setting, or <code>null</code>.
     * @see #getSubstitution( String )
     */
    public String addSubstitution(String key, String value) {
        return (String) m_substitute.put(key, value);
    }

    /**
     * Obtains the setting of a substitutable variable.
     *
     * @param key is the template variable name to query for.
     * @return the current setting, or <code>null</code>, if the variable does not exist.
     * @see #addSubstitution( String, String )
     */
    public String getSubstitution(String key) {
        String result = null;

        if (m_substitute.containsKey(key)) {
            result = (String) m_substitute.get(key);
            if (result == null) result = new String();
        }

        return result;
    }

    /**
     * Removes a substition.
     *
     * @param key is the template variable name to query for.
     * @return the current setting, or <code>null</code>, if the variable does not exist.
     * @see #addSubstitution( String, String )
     */
    public String removeSubstitution(String key) {
        return (String) m_substitute.remove(key);
    }

    /**
     * Writes the control script head, including functions for file registration.
     *
     * @return the name of the control (master) script.
     * @throws IOException if writing to the master script somehow failes.
     */
    public String initializeControlScript() throws IOException {
        // control script output filename
        String controlScript = m_dagName + ".sh";
        File controlFile = new File(m_dirName, controlScript);
        String fullPath = controlFile.getAbsolutePath();

        // existence checks before overwriting
        if (controlFile.exists()) {
            m_log.log(
                    "planner",
                    0,
                    "Warning: Master file " + fullPath + " already exists, overwriting");
            controlFile.delete();
        }

        // open master for writing
        m_master = new BufferedWriter(new FileWriter(controlFile));

        // copy template while substituting
        m_log.log("planner", 1, "writing control script header");
        copyFromTemplate(m_master, "sp-master-1.tmpl");

        // done
        return controlScript;
    }

    /**
     * Adds scripts between stages.
     *
     * @exception IOException if adding to the master script fails for some reason.
     */
    public void intermediateControlScript() throws IOException {
        m_log.log("planner", 1, "writing control script between stages");
        copyFromTemplate(m_master, "sp-master-2.tmpl");
    }

    /**
     * Write the control script tail to the control file.
     *
     * @exception IOException if adding to the master script fails for some reason.
     */
    public void finalizeControlScript() throws IOException {
        m_log.log("planner", 1, "writing control script tail");
        copyFromTemplate(m_master, "sp-master-3.tmpl");

        // close master
        m_master.flush();
        m_master.close();
        m_master = null;
    }

    /**
     * Converts a variable into the substituted value. Most of this is just a hash lookup, but some
     * are more dynamic.
     *
     * @param key is the variable to replace
     * @return the replacement string, which may be empty, never <code>null</code>.
     */
    private String convertVariable(String key) {
        if (key.equals("NOW")) {
            return Currently.iso8601(false, true, false, new Date());
        } else {
            return getSubstitution(key);
        }
    }

    /**
     * Copies a template file into the open writer. During copy, certain substitutions may take
     * place. The substitutable variables are dynamically adjusted from the main class.
     *
     * @param w is the writer open for writing.
     * @param tfn is the template base file name.
     * @throws IOException in case some io operation goes wrong.
     */
    public void copyFromTemplate(Writer w, String tfn) throws IOException {
        // determine location
        File source = new File(m_dataDir, tfn);
        if (source.exists()) {
            // template exists, use it
            LineNumberReader lnr = new LineNumberReader(new FileReader(source));

            String line, key, value;
            while ((line = lnr.readLine()) != null) {
                StringBuffer sb = new StringBuffer(line);

                // substitute all substitutables
                int circuitBreaker = 0;
                for (int p1 = sb.indexOf("@@"); p1 != -1; p1 = sb.indexOf("@@")) {
                    int p2 = sb.indexOf("@@", p1 + 2) + 2;
                    if (p2 == -1) throw new IOException("unclosed @@var@@ element");
                    key = sb.substring(p1 + 2, p2 - 2);
                    if ((value = convertVariable(key)) == null) {
                        // does not exist
                        m_log.log(
                                "planner",
                                0,
                                "Warning: "
                                        + source
                                        + ":"
                                        + lnr.getLineNumber()
                                        + ": Requesting unknown substitution for "
                                        + key);
                        value = new String();
                    } else {
                        // protocol substitution
                        m_log.log("planner", 3, "Substituting " + key + " => " + value);
                    }
                    sb.replace(p1, p2, value);

                    if (++circuitBreaker > 32) {
                        m_log.log(
                                "planner",
                                0,
                                "Warning: " + lnr.getLineNumber() + ": circuit breaker triggered");
                        break;
                    }
                }

                w.write(sb.toString());
                w.write(newline);
            }

            // free file handle resource
            lnr.close();
        } else {
            // template does not exist
            throw new IOException("template " + tfn + " not found");
        }
    }

    /**
     * Processes each job in the adag. Also checks for input file existence, if necessary.
     *
     * @param jobID is the DAX-unique job id to generate a scripts for.
     * @param checkInputFiles if set, checks in the filesystem for the existence of all input files
     *     into the job.
     * @return the name of the job control script.
     * @throws IOException for failure to write any job related files.
     */
    public String processJob(String jobID, boolean checkInputFiles) throws IOException {
        Logging.instance().log("planner", 0, "processing job: " + jobID);

        // get the job reference from ADAG
        Job job = m_adag.getJob(jobID);

        // script file for this job
        String scriptBase = job.getName() + "_" + jobID;
        String scriptFile = scriptBase + ".sh";

        // file to hold the output file list
        String outputList = scriptBase + ".lst";
        File of = new File(m_dirName, outputList);
        String outputFullPath = of.getAbsolutePath();
        if (of.exists()) {
            m_log.log(
                    "planner",
                    0,
                    "Warning: output list file " + outputList + " already exists, overwriting");
            of.delete();
        }

        // add to substitutions - temporarily
        addSubstitution("JOBSCRIPT", scriptFile);
        addSubstitution("FILELIST", outputList);
        addSubstitution("JOBID", jobID);
        addSubstitution(
                "TR", Separator.combine(job.getNamespace(), job.getName(), job.getVersion()));
        addSubstitution(
                "DV", Separator.combine(job.getDVNamespace(), job.getDVName(), job.getDVVersion()));

        // create file with all mappings for just this job
        BufferedWriter obw = new BufferedWriter(new FileWriter(of));
        Map lfnMap = new HashMap(); // store mappings for job

        for (Iterator i = job.iterateUses(); i.hasNext(); ) {
            Filename fn = (Filename) i.next();
            String lfn = fn.getFilename();

            // look up LFN in hash
            String pfn = (String) m_filenameMap.get(lfn);
            if (pfn == null) {
                // can't find the lfn in the filename list
                m_log.log(
                        "planner",
                        0,
                        "ERROR: LFN "
                                + lfn
                                + "is not in the "
                                + "<filename> list, please check the DAX!");
                return null;
            } else {
                lfnMap.put(lfn, pfn);
            }

            // check if input files exist
            if (checkInputFiles) {
                if (fn.getLink() == LFN.INPUT) {
                    if (!(new File(pfn)).canRead()) {
                        m_log.log("planner", 0, "Warning: Unable to read LFN " + lfn);
                    }
                }
            }

            // write the output file list entry: LFN PFN [abs]
            if (fn.getLink() == LFN.OUTPUT) {
                obw.write(lfn + " " + pfn + newline);
            }
        }

        // finish writing file of output files
        obw.flush();
        obw.close();

        // generate the script for this job
        boolean result = generateJobScript(job, scriptFile, outputList, lfnMap);
        if (result) {
            // OK: now add script invocation to master
            m_log.log("planner", 1, "adding job " + jobID + " to master script");
            copyFromTemplate(m_master, "sp-master-job.tmpl");
        } else {
            m_log.log("planner", 0, "Warning: ignoring script " + scriptFile);
        }

        // always clean up
        removeSubstitution("JOBSCRIPT");
        removeSubstitution("FILELIST");
        removeSubstitution("JOBID");
        removeSubstitution("TR");
        removeSubstitution("DV");

        return (result ? scriptFile : null);
    }

    /**
     * Extracts all profiles contained within the job description.
     *
     * @param job is the job description from the DAX
     * @param lfnMap is the mapping to PFNs.
     * @return a map of maps. The outer map is indexed by the lower-cased namespace identifier. The
     *     inner map is indexed by the key within the particular namespace. An empty map is
     *     possible.
     */
    private Map extractProfilesFromJob(Job job, Map lfnMap) {
        Map result = new HashMap();
        Map submap = null;

        for (Iterator i = job.iterateProfile(); i.hasNext(); ) {
            org.griphyn.vdl.dax.Profile p = (org.griphyn.vdl.dax.Profile) i.next();
            String ns = p.getNamespace().trim().toLowerCase();
            String key = p.getKey().trim();

            // recreate the vlaue
            StringBuffer sb = new StringBuffer(8);
            for (Iterator j = p.iterateLeaf(); j.hasNext(); ) {
                Leaf l = (Leaf) j.next();
                if (l instanceof PseudoText) {
                    sb.append(((PseudoText) l).getContent());
                } else {
                    String lfn = ((Filename) l).getFilename();
                    sb.append((String) lfnMap.get(lfn));
                }
            }
            String value = sb.toString().trim();

            // insert at the right place into the result map
            if (result.containsKey(ns)) {
                submap = (Map) result.get(ns);
            } else {
                result.put(ns, (submap = new HashMap()));
            }
            submap.put(key, value);
        }

        return result;
    }

    /**
     * Combines profiles from two map of maps, with regards to priority.
     *
     * @param high is the higher priority profile
     * @param low is the lower priority profile
     * @return a new map with the combination of the two profiles
     */
    private Map combineProfiles(Map high, Map low) {
        Set allKeys = new TreeSet(low.keySet());
        allKeys.addAll(high.keySet());

        Map result = new HashMap();
        for (Iterator i = allKeys.iterator(); i.hasNext(); ) {
            String key = (String) i.next();
            boolean h = high.containsKey(key);
            boolean l = low.containsKey(key);
            if (h && l) {
                Map temp = new HashMap((Map) low.get(key));
                temp.putAll((Map) high.get(key));
                result.put(key, temp);
            } else {
                if (h) result.put(key, high.get(key));
                else result.put(key, low.get(key));
            }
        }

        return result;
    }

    /**
     * Extracts the environment settings from the combined profiles.
     *
     * @param profiles is the combined profile map of maps
     * @return a string with combined profiles, or <code>null</code>, if not applicable.
     */
    private String extractEnvironment(Map profiles) {
        String result = null;

        if (profiles.containsKey("env")) {
            StringBuffer sb = new StringBuffer();
            Map env = (Map) profiles.get("env");
            for (Iterator i = env.keySet().iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                String value = (String) env.get(key);

                sb.append(key).append("='").append(value);
                sb.append("'; export ").append(key).append(newline);
            }
            result = sb.toString();
        }

        return result;
    }

    /**
     * Generates the script for each job.
     *
     * @param job is an ADAG job for which to generate the script.
     * @param scriptFile is the basename of the script for the job.
     * @param outputList is the name of a file containing output files.
     * @param lfnMap is a map of LFN to PFN.
     * @return true if all is well, false to signal an error
     */
    private boolean generateJobScript(Job job, String scriptFile, String outputList, Map lfnMap)
            throws IOException {
        String jobID = job.getID();
        File f = new File(m_dirName, scriptFile);
        String scriptFullPath = f.getAbsolutePath();
        if (f.exists()) {
            m_log.log(
                    "planner",
                    1,
                    "Warning: Script file " + scriptFile + " already exists, overwriting");
            f.delete();
        }

        // kickstart output file
        // String kickLog = scriptFullPath.substring(0,scriptFullPath.length()-3) + ".out";
        String kickLog = scriptFile.substring(0, scriptFile.length() - 3) + ".out";

        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        copyFromTemplate(bw, "sp-job-1.tmpl");

        // full definition name of this job's transformation
        String fqdn = Separator.combine(job.getNamespace(), job.getName(), job.getVersion());

        // extract TR profiles
        Map tr_profiles = extractProfilesFromJob(job, lfnMap);

        // lookup job in TC
        List tc = m_tc.lookup(job.getNamespace(), job.getName(), job.getVersion(), "local");
        if (tc == null || tc.size() == 0) {
            m_log.log(
                    "planner",
                    0,
                    "ERROR: Transformation " + fqdn + " on site \"local\" not found in TC");
            return false;
        } else if (tc.size() > 1) {
            m_log.log(
                    "planner",
                    0,
                    "Warning: Found " + tc.size() + " matches for " + fqdn + " in TC, using first");
        }
        TransformationCatalogEntry tce = (TransformationCatalogEntry) tc.get(0);

        // extract SC profiles
        Map sc_profiles = (m_sc == null ? new HashMap() : m_sc.getProfiles());

        // extract TC profiles
        Map tc_profiles = m_tc.getProfiles(tce);

        // combine profiles by priority
        Map temp = combineProfiles(tc_profiles, sc_profiles);
        Map profiles = combineProfiles(temp, tr_profiles);

        // pfnHint has been deprecated !
        if (profiles.containsKey("hints")) {
            m_log.log(
                    "planner",
                    0,
                    "Warning: The hints profile namespace "
                            + "has been deprecated, ignoring keys "
                            + ((Map) profiles.get("hints")).keySet().toString());
        }

        // assemble environment variables from profile
        String executable = tce.getPhysicalTransformation();
        String environment = extractEnvironment(profiles);

        // for web service
        boolean service = profiles.containsKey("ws");
        String invokews = null;
        String wsenv = null;

        if (service) {
            // lookup special web service invocation executable
            tc = m_tc.lookup(null, "invokews", null, "local");
            if (tc == null || tc.size() == 0) {
                // not found
                m_log.log("planner", 0, "ERROR: Transformation invokews not found!");
                return false;
            } else if (tc.size() > 1) {
                m_log.log(
                        "planner",
                        0,
                        "Warning: Found " + tc.size() + " matches for invokews in TC, using first");
            }
            tce = (TransformationCatalogEntry) tc.get(0);
            invokews = tce.getPhysicalTransformation();

            // combine profiles by priority
            temp = combineProfiles(m_tc.getProfiles(tce), sc_profiles);
            // wsenv = extractEnvironment( combineProfiles( temp, tr_profiles ) );
            wsenv = extractEnvironment(temp);
        }

        // collect commandline arguments for invocation
        StringBuffer argument = new StringBuffer();
        for (Iterator i = job.iterateArgument(); i.hasNext(); ) {
            Leaf l = (Leaf) i.next();
            if (l instanceof PseudoText) {
                argument.append(((PseudoText) l).getContent());
            } else {
                String lfn = ((Filename) l).getFilename();
                argument.append(lfnMap.get(lfn));
            }
        }

        StringBuffer ks_arg = null;
        if (m_kickstart != null) {
            ks_arg = new StringBuffer(80);
            ks_arg.append("-R local -l ").append(kickLog);
            ks_arg.append(" -n \"").append(getSubstitution("TR"));
            ks_arg.append("\" -N \"").append(getSubstitution("DV"));
            ks_arg.append('"');
        }

        // process stdin
        Filename fn = job.getStdin();
        if (fn != null) {
            if (m_kickstart != null) {
                ks_arg.append(" -i ").append((String) lfnMap.get(fn.getFilename()));
            } else {
                argument.append(" < ").append((String) lfnMap.get(fn.getFilename()));
            }
        }

        // process stdout
        fn = job.getStdout();
        if (fn != null) {
            if (m_kickstart != null) {
                ks_arg.append(" -o ").append((String) lfnMap.get(fn.getFilename()));
            } else {
                argument.append(" > ").append((String) lfnMap.get(fn.getFilename()));
            }
        }

        // process stderr
        fn = job.getStderr();
        if (fn != null) {
            if (m_kickstart != null) {
                ks_arg.append(" -e ").append((String) lfnMap.get(fn.getFilename()));
            } else {
                argument.append(" 2> ").append((String) lfnMap.get(fn.getFilename()));
            }
        }

        // environment of job
        if (environment != null) {
            bw.write("# regular job environment setup" + newline + environment + newline);
        }

        if (service) {
            //
            // web service invocation
            //
            Map in = (Map) profiles.get("ws");
            Map out = new HashMap(in.size());
            for (Iterator i = in.keySet().iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                String value = (String) in.get(key);
                out.put(key.trim().toLowerCase(), value.trim());
            }

            // check that all required arguments are present
            if (!(out.containsKey("porttype")
                    && out.containsKey("operation")
                    && out.containsKey("input"))) {
                m_log.log(
                        "planner",
                        0,
                        "ERROR: You must specify portType, operation, and input "
                                + "for a web service invocation!");
                return false;
            }

            // extra environment for web service?
            if (wsenv != null) {
                bw.write("# extra WS invocation environment" + newline + wsenv + newline);
            }

            // invocation of web service
            bw.write(invokews + " -I " + out.get("input"));
            if (out.containsKey("output")) bw.write(" -O " + out.get("output"));

            // rest of invocation
            bw.write(
                    " -p "
                            + out.get("porttype")
                            + " -o "
                            + out.get("operation")
                            + " "
                            + executable
                            + newline);
        } else {
            //
            // call the executable with argument in the script
            //
            if (m_kickstart != null) bw.write(m_kickstart + " " + ks_arg.toString() + " ");

            bw.write(executable + " " + argument + newline);
        }

        copyFromTemplate(bw, "sp-job-3.tmpl");

        // done
        bw.flush();
        bw.close();
        return true;
    }
}
