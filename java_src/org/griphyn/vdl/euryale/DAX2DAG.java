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
package org.griphyn.vdl.euryale;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Separator;
import gnu.getopt.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to convert in streaming mode information from an abstract DAG in XML (DAX)
 * into a DAGMan .dag file and a couple of related files, i.e. Condor submit files and planner
 * control files. The parser converts the DAX document specified in the commandline.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see DAXParser
 * @see org.griphyn.vdl.dax.ADAG
 */
public class DAX2DAG implements Callback {
    /** Stores the current version number for whatever purposes. */
    public static final String c_version = "$Revision$";

    /** Stores the digested version number from the class constant. */
    private String m_version;

    /** Stores the completed DAX label hyphen index as basename. */
    private String m_label;

    /** Remembers the filename of the .dag file. */
    private File m_dagname;

    /** Stores an instance to the .dag file to write in steps. */
    private PrintWriter m_dagfile;

    /** The location of the program to run as the DAGMan prescript. */
    private File m_prescript;

    /** The location of the program to run as the DAGMan postscript. */
    private File m_postscript;

    /** The number of retries per job node. */
    private int m_retries = 5;

    /** Stores an instance to a logger. */
    private Logging m_log;

    /** Start time to use in time stamping. */
    private Date m_timestamp;

    /** Printable version of the {@link #m_timestamp} above. */
    private String m_cooked_stamp;

    /**
     * Maintains the directory where to put the output files. Will be dynamically created, if it
     * does not exist.
     */
    private FlatFileFactory m_factory;

    /** Maintains the base directory until the file factory can be instantiated. */
    private File m_basedir;

    /**
     * Maintains the dynamically generated name of the common Condor logfile in a temporary
     * directory. Singleton pattern.
     */
    private String m_logfile;

    /** Maintains a minimum level for the hashed file factory to be used during instantiation. */
    private int m_minlevel;

    /** Records the location of the workflow configuration file */
    private File m_wfrc;

    /** Maintains the submit file template's filename. */
    private String m_sftFilename;

    /**
     * Maintains the in-memory copy of the submit file template. This is expected to be no larger
     * than 2kB, thus an in-memory copy should work a lot faster than continually re-reading the
     * file.
     */
    private ArrayList m_sft;

    /** Maintains the kickstart V2 config file template's filename. */
    private String m_cftFilename;

    /**
     * Maintains the in-memory copy of an optional config file template. This is expected to be no
     * larger than 2kB, thus an in-memory copy should work a lot faster than continually re-reading
     * the file.
     */
    private ArrayList m_cft;

    /** Verbosity level of messages that go onto the "app" logging queue. */
    private int m_verbosity;

    /** Maintains the properties to properly address workflow concerns. */
    private Properties m_props;

    /** Maintains a set of all jobs seen here. */
    private Set m_job;

    /** Maintains the relation of jobs to one another. */
    private Map m_parent;

    private Map m_child;

    /**
     * Set some defaults, should values be missing in the dataset. This method will only copy the
     * properties starting with the "wf." prefix, and look for VDS logging related properties.
     *
     * @param from is the initial set of properties to use for copying.
     * @return a set of properties derived from system properties.
     * @see java.lang.System#getProperties()
     */
    private Properties defaultProperties(Properties from) {
        // initial
        Properties result = new Properties();
        Pattern pattern = Pattern.compile("\\$\\{[-a-zA-Z0-9._]+\\}");

        // copy wf keys as specified in the system properties to defaults
        for (Enumeration e = from.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            String value = from.getProperty(key);

            if (key.startsWith("wf.") || key.startsWith("work.")) {
                // unparse value ${prop.key} inside braces
                Matcher matcher = pattern.matcher(value);
                StringBuffer sb = new StringBuffer();
                boolean found = false;
                while (matcher.find()) {
                    // extract name of properties from braces
                    String newKey = value.substring(matcher.start() + 2, matcher.end() - 1);

                    // try to find a matching value in result properties
                    String newVal =
                            result.getProperty(
                                    newKey, from.getProperty(newKey, System.getProperty(newKey)));

                    // replace braced string with the actual value or empty string
                    matcher.appendReplacement(sb, newVal == null ? "" : newVal);

                    // for later
                    found = true;
                }

                matcher.appendTail(sb);
                result.setProperty(key, sb.toString());
            }

            if (key.startsWith("vds.")) {
                if (key.equals("vds.verbose")) m_log.setVerbose(Integer.parseInt(value));
                else if (key.startsWith("vds.log.")) {
                    m_log.register(key.substring(8), value);
                }
            }
        }

        // final
        return result;
    }

    private static String catfile(String d1, String d2, String fn) {
        File f1 = new File(d1, d2);
        File f2 = new File(f1, fn);
        return f2.getPath();
    }

    /**
     * Constructs a new instance of the converter and reads properties from the default position.
     */
    public DAX2DAG() {
        // start logging
        m_log = Logging.instance();
        m_verbosity = 0;

        m_timestamp = new Date();
        m_cooked_stamp = null;
        m_label = null;
        m_dagname = null;
        m_dagfile = null;
        m_logfile = null;
        m_version = c_version.substring(10, c_version.length() - 1).trim();
        m_props = defaultProperties(System.getProperties());
        m_wfrc = new File(System.getProperty("user.home", "."), ".wfrc");
        m_cft = null;
        m_sft = null;

        String vds_home = m_props.getProperty("vds.home", System.getProperty("vds.home"));
        m_sftFilename = catfile(vds_home, "share", "grid3.sft");

        File libexec = new File(vds_home, "libexec");
        m_prescript = new File(libexec, "prescript.pl");
        m_postscript = new File(libexec, "postscript.pl");

        if (m_log.isUnset("app")) {
            m_verbosity = 0;
            m_log.register("app", System.out, m_verbosity);
        } else {
            m_verbosity = m_log.getLevel("app");
            if (m_verbosity == Integer.MAX_VALUE || m_verbosity < 0) {
                m_verbosity = 0;
                m_log.setLevel("app", m_verbosity);
            }
        }

        // new
        m_job = new HashSet();
        m_parent = new HashMap();
        m_child = new HashMap();

        // create files in current directory, unless anything else is known.
        m_basedir = new File(".");
        try {
            m_factory = new FlatFileFactory(m_basedir); // minimum default
        } catch (IOException io) {
            m_log.log("default", 0, "WARNING: Unable to generate files in the CWD");
        }
        m_minlevel = -1;
    }

    /**
     * Increases the verbosity of the app logging queue.
     *
     * @return the current level.
     */
    public int increaseVerbosity() {
        this.m_log.setLevel("app", ++this.m_verbosity);
        return this.m_verbosity;
    }

    /**
     * Remembers which workflow property file should be chosen. It will not be read now. Only its
     * location will be remembered.
     *
     * @param wfrc is the location of a property file.
     */
    public void setWorkflowPropertyFile(File wfrc) {
        m_wfrc = wfrc;
    }

    public void finalizeProperties() {
        boolean success = false;
        Properties temp = new Properties();

        try {
            if (m_wfrc.exists() && m_wfrc.canRead()) {
                FileInputStream fis = new FileInputStream(m_wfrc);
                temp.load(fis);
                fis.close();
                success = true;
            } else {
                m_log.log("app", 0, "WARNING: No wfrc property file found!");
            }
        } catch (IOException io) {
            m_log.log(
                    "default",
                    0,
                    "WARNING: Error while reading properties " + m_wfrc + ": " + io.getMessage());
        }

        // replace, if we were able to read, and if there is anything
        // available in the new property set.
        Properties p = defaultProperties(temp);
        p.putAll(m_props);
        m_props = p;

        // init property-dependent member variables
        String r = m_props.getProperty("wf.job.retries");
        if (r != null) m_retries = Integer.parseInt(r);

        // some more sanity checking
        if ((r = m_props.getProperty("wf.script.pre")) == null) {
            if (m_prescript == null)
                throw new RuntimeException("ERROR: Unable to determine a pre-script location");
        } else {
            m_prescript = new File(r);
        }

        if ((r = m_props.getProperty("wf.script.post")) == null) {
            if (m_postscript == null)
                throw new RuntimeException("ERROR: Unable to determine a post-script location");
        } else {
            m_postscript = new File(r);
        }
    }

    /**
     * Allows to set a property from the code.
     *
     * @param key is the property key
     * @param value is the new value to store
     * @return the previous value, or null
     */
    public String setProperty(String key, String value) {
        return (String) this.m_props.setProperty(key, value);
    }

    /**
     * Sets the DAGMan PRE script location.
     *
     * @param fn is the location of the PRE script.
     */
    public void setPrescript(String fn) {
        m_prescript = new File(fn);
        setProperty("wf.script.pre", fn);
    }

    /**
     * Sets the DAGMan POST script location.
     *
     * @param fn is the location of the POST script.
     */
    public void setPostscript(String fn) {
        m_postscript = new File(fn);
        setProperty("wf.script.post", fn);
    }

    /**
     * Sets the output directory. This directory will be dynamically created once the document
     * header is found.
     *
     * @param dir is the new directory to use
     */
    public void setDirectory(String dir) {
        m_basedir = new File(dir);
    }

    /**
     * Sets the minimum level in the hashed file factory. This is to remember until the factory
     * actually gets instantiated.
     *
     * @param level is the minimum level requested
     */
    public void setMinimumLevel(int level) {
        m_minlevel = level;
    }

    /**
     * Sets the timestamp that is being emitted in all files.
     *
     * @param then is the new date to use for the timestamping.
     * @return the previously valid timestamp.
     */
    public Date setTimestamp(Date then) {
        Date old = m_timestamp;
        m_timestamp = then;
        m_cooked_stamp = Currently.iso8601(true, false, false, m_timestamp);
        return old;
    }

    /**
     * Reads the submit file template into memory for submit file generation.
     *
     * @param sft is a file that contains the submit file template
     * @return false if unable to read the submit file template
     */
    public boolean setSubmitFileTemplate(File sft) {
        boolean result = false;

        try {
            String line;
            ArrayList temp = new ArrayList();

            BufferedReader br = new BufferedReader(new FileReader(sft));
            while ((line = br.readLine()) != null) temp.add(line);
            br.close();

            // switch now on success
            result = true;
            m_sftFilename = sft.getCanonicalPath();
            m_sft = temp;
        } catch (IOException io) {
            System.err.println(
                    "ERROR: Unable to read submit file template " + sft + ": " + io.getMessage());
            System.exit(3);
        }

        return result;
    }

    /**
     * Reads the configuration file template into memory for kickstart V2 file generation. This
     * function is only activated, if kickstart v2 configuration is being requested.
     *
     * @param cft is the file that contains the config file template
     * @return false if unable to read the config file template
     */
    public boolean setConfigFileTemplate(File cft) {
        boolean result = false;

        try {
            String line;
            ArrayList temp = new ArrayList();

            BufferedReader br = new BufferedReader(new FileReader(cft));
            while ((line = br.readLine()) != null) temp.add(line);
            br.close();

            // switch now on success
            result = true;
            m_cftFilename = cft.getCanonicalPath();
            m_cft = temp;
        } catch (IOException io) {
            System.err.println(
                    "ERROR: Unable to read config file template " + cft + ": " + io.getMessage());
            System.exit(4);
        }

        return result;
    }

    /**
     * Callback when the opening tag was parsed. The attribute maps each attribute to its raw value.
     * The callback initializes the DAG writer.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cb_document(java.util.Map attributes) {
        m_log.log("dax2dag", 1, "got attributes " + attributes.toString());

        // extract the label of the dax
        if ((this.m_label = (String) attributes.get("name")) == null) this.m_label = "test";
        // create a temporary filename for the common log file
        try {
            this.m_logfile = File.createTempFile(m_label + "-", ".log", null).getAbsolutePath();
        } catch (IOException e) {
            // use local, relative entry
            this.m_logfile = m_label + ".log";
        }

        // extract the index/count of the dax, usually 0
        String index = (String) attributes.get("index");
        if (index == null) index = "0";

        // create the complete label to name the .dag file
        this.m_label += "-" + index;

        // create hashed, and levelled directories
        String s = (String) attributes.get("jobCount");
        try {
            HashedFileFactory temp = null;
            int jobCount = (s == null ? 0 : Integer.parseInt(s));
            if (m_minlevel > 0 && m_minlevel > jobCount) jobCount = m_minlevel;
            if (jobCount > 0) temp = new HashedFileFactory(m_basedir, jobCount);
            else temp = new HashedFileFactory(m_basedir);

            m_factory = temp;
            m_log.log("default", 0, "using " + temp.getLevels() + " directory levels");
        } catch (NumberFormatException nfe) {
            if (s == null) System.err.println("ERROR: Unspecified number for jobCount");
            else System.err.println("ERROR: Illegal number \"" + s + "\" for jobCount");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("ERROR: Base directory creation");
            System.err.println(e.getMessage());
            System.exit(1);
        }

        // create dag filename
        try {
            m_dagname = m_factory.createFlatFile(this.m_label + ".dag");
        } catch (IOException io) {
            System.err.println("Unable to create a flat filename for the DAG: " + io.getMessage());
            System.exit(1);
        }

        // open dag writer
        m_log.log("dax2dag", 2, "open dag writer " + m_dagname);
        try {
            if (m_dagname.exists())
                m_log.log("default", 0, "WARNING: Overwriting file " + m_dagname);
            m_dagfile = new PrintWriter(new FileWriter(m_dagname));

            m_dagfile.println("# dax2dag " + m_version);
            m_dagfile.print("## ");
            if (m_dagname.getParent() != null) m_dagfile.println("cd " + m_dagname.getParent());
            m_dagfile.println("## vds-submit-dag " + m_dagname.getName());

            m_dagfile.println("# " + Currently.iso8601(false, true, false, m_timestamp));
            m_dagfile.println("#");
        } catch (IOException io) {
            System.err.println("Unable to open DAG " + m_dagname + ": " + io.getMessage());
            System.exit(1);
        }
    }

    /**
     * Callback when the section 1 filenames are being parsed. This is unused by design, as the
     * reduction of a DAG according to the existence of files happens dynamically.
     *
     * @param filename is a DAX-style filename elements.
     */
    public void cb_filename(Filename filename) {
        // m_log.log( "dax2dag", 1, "filename callback " + filename.getFilename() );
    }

    /**
     * Converts the dontRegister and dontTransfer flags into a numeric value of reverse meaning.
     *
     * <table>
     * <tr><th>dR</th><th>dT</th><th>result</th></tr>
     * <tr><td>false</td><td>0</td><td>0</td></tr>
     * <tr><td>false</td><td>1</td><td>1</td></tr>
     * <tr><td>false</td><td>2</td><td>2</td></tr>
     * <tr><td>true</td><td>0</td><td>4</td></tr>
     * <tr><td>true</td><td>1</td><td>5</td></tr>
     * <tr><td>true</td><td>2</td><td>6</td></tr>
     * </table>
     *
     * @param dontRegister true for unregistered files.
     * @param dontTransfer for the chosen transfer mode.
     * @return the numerical representation.
     */
    private int assembleRT(boolean dontRegister, int dontTransfer) {
        int result = dontTransfer; // range 0..2
        if (!dontRegister) result |= 0x04;
        return result;
    }

    /**
     * Replaces a true logical filename with a construct that is late bound to the true file. Thus,
     * the output is a !!var!! like:
     *
     * <p>
     *
     * <pre>!!LFN:filename!!</pre>
     *
     * @param f is the logical filename DAX construct.
     * @return a String for a late binding replacement
     */
    private String convertFilename(Filename f) {
        StringBuffer result = new StringBuffer(32);

        result.append("!!LFN:");
        result.append(f.getFilename());
        result.append("!!");

        return result.toString();
    }

    /**
     * Converts a dax leaf element into something to output.
     *
     * @param l is a leaf element
     * @return the printable version of the leaf, or an empty string.
     */
    private String convertLeaf(Leaf l) {
        if (l instanceof PseudoText) {
            return ((PseudoText) l).getContent();
        } else if (l instanceof Filename) {
            return convertFilename((Filename) l);
        } else {
            // FIXME: complain
            return new String();
        }
    }

    /**
     * Converts a given @@key@@ variable into its replacement value. Only a fixed set of variables
     * are hard-coded in this method.
     *
     * <table>
     * <tr><th>key</th><th>meaning</th></tr>
     * <tr><td>ARGS</td><td>is from job/argument, may be empty</td></tr>
     * <tr><td>CONFIG</td><td>is the k2 config filename</td></tr>
     * <tr><td>LOGFILE</td><td>is the log file all submit files share.
     * Note: For reasons for NFS locking, this file should reside on a
     * local filesystem.</td></tr>
     * <tr><td>DAGFILE</td><td>is the filename of the DAGMan .dag file</td></tr>
     * <tr><td>DAXLABEL</td><td>is the adag@label value</td></tr>
     * <tr><td>DAXMTIME</td><td>is the some time assoc. with the .dax file</td></tr>
     * <tr><td>DV</td><td>is the combined job@dv-{namespace|name|version}</td></tr>
     * <tr><td>GENERATOR</td><td>Name of the generator</td></tr>
     * <tr><td>JOBID</td><td>is the job@id value for this job</td></tr>
     * <tr><td>LEVEL</td><td>is the job@level value for this job</td></tr>
     * <tr><td>MAXPEND</td><td>is the maximum time a job is willing to pend
     * remotely (spend in idle on the local Condor) before it is being
     * replanned. Defaults to 2 hours.</td>
     * <tr><td>STDIN</td><td>is the optional LFN from the job/stdin filename</td></tr>
     * <tr><td>STDOUT</td><td>is the optional LFN from the job/stdout filename</td></tr>
     * <tr><td>STDERR</td><td>is the optional LFN from the job/stderr filename</td></tr>
     * <tr><td>SUBMIT</td><td>is the submit filename</td></tr>
     * <tr><td>SUBBASE</td><td>is the submit filename minus the .sub suffix</td></tr>
     * <tr><td>TEMPLATE</td><td>is the submit filename template name</td></tr>
     * <tr><td>TR</td><td>is the combined job@{namespace|name|version}</td></tr>
     * <tr><td>VERSION</td><td>for starters 1.0 will do</td></tr>
     * </table>
     *
     * @param key is the key with the at characters removed.
     * @param job is the job from which to glean additional information.
     * @param submitFilename is the filename of the submit file
     * @return the replacement, which may be an empty string.
     */
    private String convertVariable(String key, Job job, String submitFilename) {
        String result = null;
        m_log.log("dax2dag", 4, "converting key " + key);

        switch (key.charAt(0)) {
            case 'A':
                if (key.equals("ARGS")) {
                    StringBuffer arglist = new StringBuffer(32);
                    for (Iterator i = job.iterateArgument(); i.hasNext(); ) {
                        arglist.append(convertLeaf((Leaf) i.next()));
                    }
                    result = arglist.toString();
                }
                break;

            case 'C':
                if (key.equals("CONFIG")) {
                    result = submitFilename.substring(0, submitFilename.length() - 3) + "in";
                }
                break;

            case 'D':
                if (key.equals("DV")) {
                    result =
                            Separator.combine(
                                    job.getDVNamespace(), job.getDVName(), job.getDVVersion());
                } else if (key.equals("DAXLABEL")) {
                    result = this.m_label;
                } else if (key.equals("DAXMTIME")) {
                    result = this.m_cooked_stamp;
                    // Currently.iso8601(true,false,false,m_timestamp);
                } else if (key.equals("DAGFILE")) {
                    result = m_dagname.getPath();
                }
                break;

            case 'G':
                if (key.equals("GENERATOR")) {
                    result = "d2d";
                }
                break;

            case 'J':
                if (key.equals("JOBID")) {
                    result = job.getID();
                }
                break;

            case 'L':
                if (key.equals("LEVEL")) {
                    result = Integer.toString(job.getLevel());
                } else if (key.equals("LOGFILE")) {
                    result = (m_logfile == null ? m_label + ".log" : m_logfile);
                }
                break;

            case 'M':
                if (key.equals("MAXPEND")) {
                    String temp = m_props.getProperty("wf.max.pending", "7200");
                    if (Integer.parseInt(temp) >= 600) result = temp;
                    else result = "7200";
                }
                break;

            case 'S':
                if (key.equals("SUBMIT")) {
                    result = submitFilename;
                } else if (key.equals("SUBBASE")) {
                    result = submitFilename.substring(0, submitFilename.length() - 4);
                } else if (key.equals("STDIN")) {
                    if (job.getStdin() != null) result = convertFilename(job.getStdin());
                } else if (key.equals("STDOUT")) {
                    if (job.getStdout() != null) result = convertFilename(job.getStdout());
                } else if (key.equals("STDERR")) {
                    if (job.getStderr() != null) result = convertFilename(job.getStderr());
                }
                break;

            case 'T':
                if (key.equals("TR")) {
                    result = Separator.combine(job.getNamespace(), job.getName(), job.getVersion());
                } else if (key.equals("TEMPLATE")) {
                    result = m_sftFilename;
                }
                break;

            case 'V':
                if (key.equals("VERSION")) {
                    result = m_version;
                }
                break;

            default:
                // FIXME: complain
        }

        // guarantee to return a valid string and not null
        return (result == null ? new String() : result);
    }

    /**
     * Writes the job planner configuration into the submit file. The section file contains several
     * configuration sections to ease the life of the late planner.
     *
     * @param prefix is the prefix to use in front of the uses section.
     * @param sfw is an opened submit file writer.
     * @param job is the job from which to create the config file.
     * @throws IOException, if something goes wrong while opening the file.
     */
    private void writeUsesSection(String prefix, PrintWriter sfw, Job job) throws IOException {
        // section filenames, may be empty
        sfw.println(prefix + "[filenames]");
        for (Iterator i = job.iterateUses(); i.hasNext(); ) {
            Filename f = (Filename) i.next();

            // format: <io> <rt> "<lfn>"
            sfw.print(prefix);
            sfw.print(f.getLink());
            sfw.print(' ');
            sfw.print(assembleRT(f.getDontRegister(), f.getDontTransfer()));
            sfw.print(" \"");
            sfw.print(f.getFilename());
            // sfw.print( "\" \"" );
            // String temp = f.getTemporary();
            // if ( temp != null ) sfw.print( temp );
            sfw.println("\"");
        }
        sfw.println(prefix);

        // section stdio, may be empty
        sfw.println(prefix + "[stdio]");
        if (job.getStdin() != null)
            sfw.println(prefix + "stdin=" + convertFilename(job.getStdin()));
        if (job.getStdout() != null)
            sfw.println(prefix + "stdout=" + convertFilename(job.getStdout()));
        if (job.getStderr() != null)
            sfw.println(prefix + "stderr=" + convertFilename(job.getStderr()));
        sfw.println(prefix);

        // section profile, may be empty
        sfw.println(prefix + "[profiles]");
        for (Iterator i = job.iterateProfile(); i.hasNext(); ) {
            Profile p = (Profile) i.next();
            sfw.print(prefix + p.getNamespace() + "." + p.getKey() + "=\"");
            for (Iterator j = p.iterateLeaf(); j.hasNext(); ) {
                sfw.print(convertLeaf((Leaf) j.next()));
            }
            sfw.println("\"");
        }
        sfw.println(prefix);

        // section job, usually not empty
        sfw.println(prefix + "[job]");
        sfw.println(
                prefix
                        + "transformation="
                        + Separator.combine(job.getNamespace(), job.getName(), job.getVersion()));
        sfw.println(
                prefix
                        + "derivation="
                        + Separator.combine(
                                job.getDVNamespace(), job.getDVName(), job.getDVVersion()));
        sfw.println(prefix + "wf_label=" + this.m_label);
        sfw.println(prefix + "wf_time=" + this.m_cooked_stamp);

        // kickstart V2 or not
        if (m_cft != null && m_cft.size() > 0) sfw.println(prefix + "kickstart=v2");

        sfw.println(prefix);
    }

    /**
     * Writes the .sub Condor submit file. The submit file contains semi-planned job information
     * from the generic job template.
     *
     * @param submit is the location where to create the file at.
     * @param job is the job from which to create the submit file.
     * @throws IOException, if something goes wrong while opening the file.
     */
    private void writeSubmitFile(File submit, Job job) throws IOException {
        String basename = m_factory.getName(submit);
        if (submit.exists()) m_log.log("default", 0, "WARNING: Overwriting file " + submit);

        PrintWriter sub = new PrintWriter(new FileWriter(submit));
        m_log.log("dax2dag", 3, "create sub file " + submit);

        sub.println("# dax2dag " + m_version);
        sub.println("# Condor submit file " + basename);
        sub.println("# " + Currently.iso8601(false, true, false, m_timestamp));
        sub.println("#");

        // write uses information into submit file with special prefix
        String prefix = "#! ";
        sub.println(
                "## The section prefixed with \""
                        + prefix
                        + "\" passes information to the late planner.");
        sub.println("## BEGIN late planning configuration");
        writeUsesSection(prefix, sub, job);
        sub.println("## END late planning configuration");
        sub.println("#");

        // substitute from template file
        for (Iterator i = m_sft.iterator(); i.hasNext(); ) {
            StringBuffer line = new StringBuffer((String) i.next());

            // substitute all @@var@@ occurances in this line
            // FIXME: Need to introduce string quoting and escape rules eventually
            for (int p1 = line.indexOf("@@"); p1 != -1; p1 = line.indexOf("@@")) {
                int p2 = line.indexOf("@@", p1 + 2) + 2;
                if (p2 == -1) throw new IOException("unclosed @@var@@ element");
                String key = line.substring(p1 + 2, p2 - 2);
                String value = convertVariable(key, job, basename);
                m_log.log("dax2dag", 4, key + " => " + value);
                line.replace(p1, p2, value);
            }

            sub.println(line.toString());
        }

        sub.flush();
        sub.close();
    }

    /**
     * Writes the .in kickstart v2 control file. The config file contains semi-planned job
     * information from the generic config file template.
     *
     * @param config is the location where to create the file at.
     * @param submit is the name of the corresponding submit file.
     * @param job is the job from which to create the config file.
     * @throws IOException, if something goes wrong while opening the file.
     */
    private void writeConfigFile(File config, File submit, Job job) throws IOException {
        if (config.exists()) m_log.log("default", 0, "WARNING: Overwriting file " + config);

        PrintWriter cfg = new PrintWriter(new FileWriter(config));
        m_log.log("dax2dag", 3, "create k2 config file " + config);

        cfg.println("# dax2dag " + m_version);
        cfg.println("# kickstart config file " + m_factory.getName(config));
        cfg.println("# " + Currently.iso8601(false, true, false, m_timestamp));
        cfg.println("#");

        // substitute from template file
        for (Iterator i = m_cft.iterator(); i.hasNext(); ) {
            StringBuffer line = new StringBuffer((String) i.next());

            // substitute all @@var@@ occurances in this line
            // FIXME: Need to introduce string quoting and escape rules eventually
            for (int p1 = line.indexOf("@@"); p1 != -1; p1 = line.indexOf("@@")) {
                int p2 = line.indexOf("@@", p1 + 2) + 2;
                if (p2 == -1) throw new IOException("unclosed @@var@@ element");
                String key = line.substring(p1 + 2, p2 - 2);
                String value = convertVariable(key, job, m_factory.getName(submit));
                m_log.log("dax2dag", 4, key + " => " + value);
                line.replace(p1, p2, value);
            }

            cfg.println(line.toString());
        }

        cfg.flush();
        cfg.close();
    }

    /**
     * Ensures that the submit file references the submit host local config file. The function will
     * ensure that there is an <code>input</code> configuration inside the submit file, which refers
     * to the configuration file.
     */
    public void checkConfigSubmit() {
        String linefeed = System.getProperty("line.separator", "\r\n");
        boolean flag = false;

        // exchange (or add) a line "input = @@CONFIG@@" to submit file template
        for (ListIterator i = m_sft.listIterator(); i.hasNext(); ) {
            String line = ((String) i.next()).trim();
            if (line.length() > 5 && line.substring(0, 5).toLowerCase().equals("input")) {
                flag = true;
                i.set("input = @@CONFIG@@" + linefeed);
                i.add("transfer_input = mumbojumbo" + linefeed);
            }
            if (line.length() > 14
                    && line.substring(0, 14).toLowerCase().equals("transfer_input")) {
                i.set("transfer_input = true" + linefeed);
            }
        }

        if (!flag) {
            // sigh, not in the list, so prepend
            m_sft.add(0, "input = @@CONFIG@@" + linefeed);
            m_sft.add(0, "transfer_input = true" + linefeed);
        }
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely assembled, but each is
     * passed separately. For each job, the submit file needs to be created from the submit file
     * template. Furthermore, for each submit file, the kickstart control file needs to be written,
     * and some other useful files for the late planner.
     *
     * @param job is the DAX-style job.
     */
    public void cb_job(Job job) {
        String id = job.getID();
        m_log.log("dax2dag", 1, "found job " + id);

        // remember job -- to find parents and children
        m_job.add(id);

        // create and write submit file
        File submit = null;
        try {
            String fn = id + ".sub";
            submit = m_factory.createFile(fn);
            writeSubmitFile(submit, job);
        } catch (IOException io) {
            System.err.println(
                    "ERROR: Unable to write submit file " + submit + ": " + io.getMessage());
            System.exit(2);
        }

        // write kickstart.v2 config file
        if (m_cft != null) {
            // do not use factory method -- we need to go into the same dir!
            File config = new File(submit.getParentFile(), id + ".in");
            try {
                writeConfigFile(config, submit, job);
            } catch (IOException io) {
                System.err.println(
                        "ERROR: Unable to write config file " + config + ": " + io.getMessage());
                System.exit(2);
            }
        }

        // append dag file
        m_log.log("dax2dag", 3, "appending dag file with job");
        if (m_prescript == null) {
            //      String fn = m_props.getProperty("wf.script.pre");
            //      if ( fn == null )
            throw new RuntimeException("ERROR: Unable to determine location of pre-script!");
            //      m_prescript = new File(fn);
        }
        if (m_postscript == null) {
            //      String fn = m_props.getProperty("wf.script.post");
            //      if ( fn == null )
            throw new RuntimeException("ERROR: Unable to determine location of post-script!");
            //      m_postscript = new File(fn);
        }

        String basename = m_factory.getName(submit);
        String suffix = " " + basename + " ";
        try {
            suffix += m_wfrc.getCanonicalPath();
        } catch (IOException ioe) {
            m_log.log("default", 0, "ignoring un-canonicalizable " + m_wfrc.getAbsolutePath());
        }
        m_dagfile.println("JOB " + id + " " + basename);
        m_dagfile.println("SCRIPT PRE  " + id + " " + m_prescript + suffix);
        m_dagfile.println("SCRIPT POST " + id + " " + m_postscript + " -e $RETURN" + suffix);
        if (m_retries > 1) m_dagfile.println("RETRY " + id + " " + m_retries + " UNLESS-EXIT 42");
    }

    public void cb_parents(String child, java.util.List parents) {
        m_log.log("dax2dag", 1, "relationship " + child + " " + parents);

        // remember parents -- to find later the initial and final jobsets
        if (!m_parent.containsKey(child)) m_parent.put(child, new TreeSet());
        ((Set) m_parent.get(child)).addAll(parents);

        // write dependency into dag file
        // !!    m_dagfile.print( "PARENT" );
        for (Iterator i = parents.iterator(); i.hasNext(); ) {
            String parent = (String) i.next();

            if (!m_child.containsKey(parent)) m_child.put(parent, new TreeSet());
            ((Set) m_child.get(parent)).add(child);

            // !!      m_dagfile.print( " " + parent );
        }
        // !!    m_dagfile.println( " CHILD " + child );
    }

    /**
     * Attempts to find the primeval ancestor of a given job.
     *
     * @param job is the job to check for ancestors.
     * @return all ancestors found for the given job. A job without ancestors is the job itself.
     */
    private Set find_ancestor(String job) {
        Set result = new TreeSet();

        if (m_parent.containsKey(job)) {
            for (Iterator i = ((Set) m_parent.get(job)).iterator(); i.hasNext(); )
                result.addAll(find_ancestor((String) i.next()));
        } else {
            result.add(job);
        }

        return result;
    }

    /**
     * Attempts to find the youngest distant children of a given job.
     *
     * @param job is the job to check for children.
     * @return all grandchildren found for a given job. A job without children is the job itself.
     */
    private Set find_children(String job) {
        Set result = new TreeSet();

        m_log.log("dax2dag", 2, "looking up children for " + job);
        if (m_child.containsKey(job)) {
            for (Iterator i = ((Set) m_child.get(job)).iterator(); i.hasNext(); )
                result.addAll(find_children((String) i.next()));
        } else {
            result.add(job);
        }

        return result;
    }

    /**
     * Callback when the parsing of the document is done. This callback closes and frees the DAG
     * writer.
     */
    public void cb_done() {
        m_log.log("dax2dag", 2, "parent sets " + m_parent);
        m_log.log("dax2dag", 2, "child sets " + m_child);

        // print relationship now, since DAGMan likes ordering
        TreeSet temp = new TreeSet(m_parent.keySet());
        for (Iterator i = temp.iterator(); i.hasNext(); ) {
            String child = (String) i.next();
            TreeSet parents = (TreeSet) m_parent.get(child);
            if (parents.size() > 0) {
                m_dagfile.print("PARENT ");
                for (Iterator j = parents.iterator(); j.hasNext(); )
                    m_dagfile.print((String) j.next() + " ");
                m_dagfile.println("CHILD " + child);
            }
        }
        temp = null; // free

        // find all initial jobs
        Set initial = new TreeSet();
        Set cleanup = new TreeSet();
        if (m_job.size() <= 0) {
            // 0
            m_log.log("app", 0, "ERROR: There are no jobs");
        } else {
            // many: for each job, go to its original ancestor / youngest child
            for (Iterator i = m_job.iterator(); i.hasNext(); ) {
                String job = (String) i.next();
                initial.addAll(find_ancestor(job));
                cleanup.addAll(find_children(job));
            }
        }

        // for now, just pretend
        m_dagfile.print("# PARENT ID000000 CHILD");
        for (Iterator i = initial.iterator(); i.hasNext(); ) m_dagfile.print(" " + i.next());
        m_dagfile.println();

        m_dagfile.print("# PARENT");
        for (Iterator i = cleanup.iterator(); i.hasNext(); ) m_dagfile.print(" " + i.next());
        m_dagfile.println(" CHILD ID999999");

        // done
        m_dagfile.flush();
        m_dagfile.close();
    }

    public void showFinals() {
        m_log.log("default", 0, "created " + m_factory.getCount() + " structured filenames.");
        m_log.log("default", 0, "created " + m_factory.getFlatCount() + " flat filenames.");
    }

    // -----------------------------------------------------------------

    public void showUsage() {
        String basename = this.getClass().getName();
        int p = basename.lastIndexOf('.');
        if (p != -1) basename = basename.substring(p + 1);

        String linefeed = System.getProperty("line.separator", "\r\n");
        System.out.println("$Id$");
        System.out.println(
                "Usage: "
                        + basename
                        + " [-d dir] [-V] [-w wfrc] [-P pre] [-p post] [-l min] [-t sft] dax");
        System.out.println(
                linefeed
                        + "Mandatory arguments: "
                        + linefeed
                        + " dax                name of the DAX file to plan."
                        + linefeed
                        + linefeed
                        + "Optional arguments: "
                        + linefeed
                        + " -d|--dir dir       directory in which to generate the file, default is \".\""
                        + linefeed
                        + " -w|--wfrc rcfile   workflow properties location, default is $HOME/.wfrc"
                        + linefeed
                        + " -P|--prescript fn  name of the late-planning DAGMan prescript file."
                        + linefeed
                        + " -p|--postscript fn name of the late-planning DAGMan postscript file."
                        + linefeed
                        + " -t|--template sft  submit file template to use."
                        + linefeed
                        + "                    default: "
                        + m_sftFilename
                        + linefeed
                        + " -l|--levels min    minimum number of levels in directory structure (0..3)."
                        + linefeed
                        + " -V|--version       print version information and exit."
                        + linefeed
                        + " -v|--verbose       increases output verbosity."
                        + linefeed);
        System.out.println(
                "It is recommended to always use the dir option with a sensible argument. The"
                        + linefeed
                        + "wfrc properties usually specify the location of the pre- and post-script."
                        + linefeed
                        + "The number of subdirectory levels is automatically determined from the number"
                        + linefeed
                        + "of jobs."
                        + linefeed);
    }

    /**
     * Creates a set of long options to use.
     *
     * @return initialized long options.
     */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[11];

        lo[0] = new LongOpt("prescript", LongOpt.REQUIRED_ARGUMENT, null, 'P');
        lo[1] = new LongOpt("postscript", LongOpt.REQUIRED_ARGUMENT, null, 'p');
        lo[2] = new LongOpt("dir", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[3] = new LongOpt("wfrc", LongOpt.REQUIRED_ARGUMENT, null, 'w');
        lo[4] = new LongOpt("template", LongOpt.REQUIRED_ARGUMENT, null, 't');
        lo[5] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[6] = new LongOpt("k.2", LongOpt.REQUIRED_ARGUMENT, null, '2');
        lo[7] = new LongOpt("k2", LongOpt.REQUIRED_ARGUMENT, null, '2');
        lo[8] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[9] = new LongOpt("levels", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        lo[10] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        return lo;
    }

    /**
     * Point of entry to convert the DAX into DAG with helper and submit files.
     *
     * @param args are the commandline arguments.
     */
    public static void main(String[] args) {
        DAX2DAG me = new DAX2DAG();
        if (args.length == 0) {
            me.showUsage();
            return;
        }

        Getopt opts = new Getopt("DAX2DAG", args, "2:P:Vd:hp:t:w:v", me.generateValidOptions());
        opts.setOpterr(false);
        boolean sftIsSet = false;
        boolean cftIsSet = false;
        String arg = null;
        int option = 0;
        while ((option = opts.getopt()) != -1) {
            switch (option) {
                case '2':
                    if ((arg = opts.getOptarg()) != null) {
                        File cft = new File(arg);
                        if (!cft.exists() || !cft.canRead()) {
                            System.err.println("ERROR: Unable to read config template " + cft);
                            System.exit(1);
                        }
                        me.setConfigFileTemplate(cft);
                        cftIsSet = true;
                    }
                    break;

                case 'P':
                    if ((arg = opts.getOptarg()) != null) me.setPrescript(arg);
                    break;

                case 'V':
                    System.out.println("$Id$");
                    return;

                case 'd':
                    if ((arg = opts.getOptarg()) != null && arg.length() > 0) me.setDirectory(arg);
                    break;

                case 'l':
                    if ((arg = opts.getOptarg()) != null && arg.length() > 0) {
                        int level;
                        try {
                            level = Integer.parseInt(arg);
                        } catch (NumberFormatException nfe) {
                            level = -1;
                        }
                        if (level >= 0 && level <= 3) me.setMinimumLevel(level);
                        else System.out.println("Ignoring illegal minimum level of " + level);
                    }
                    break;

                case 'p':
                    if ((arg = opts.getOptarg()) != null) me.setPostscript(arg);
                    break;

                case 't':
                    if ((arg = opts.getOptarg()) != null) {
                        File sft = new File(arg);
                        if (!sft.exists() || !sft.canRead()) {
                            System.err.println("ERROR: Cannot read template " + sft);
                            System.exit(1);
                        }
                        me.setSubmitFileTemplate(sft);
                        sftIsSet = true;
                    }
                    break;

                case 'w':
                    if ((arg = opts.getOptarg()) != null) me.setWorkflowPropertyFile(new File(arg));
                    break;

                case 'v':
                    me.increaseVerbosity();
                    break;

                case 'h':
                default:
                    me.showUsage();
                    return;
            }
        }

        // post CLI args checks
        if (!sftIsSet) {
            File sft = new File(me.m_sftFilename);
            if (!sft.exists() || !sft.canRead()) {
                System.err.println(
                        "ERROR: No valid template file found. Please use -t to point\n"
                                + "to a valid and accessible submit file template location.");
                System.exit(1);
            }
            me.setSubmitFileTemplate(sft);
            sftIsSet = true;
        } else {
            Logging.instance().log("default", 0, "starting");
        }

        // finalize dangling properties
        try {
            me.finalizeProperties();
        } catch (RuntimeException rte) {
            System.err.println(rte.getMessage());
            System.err.println("Likely cause: Are your wfrc properties accessible?");
            System.exit(1);
        }

        // kickstart v2?
        if (cftIsSet) me.checkConfigSubmit();

        if (opts.getOptind() != args.length - 1) {
            System.err.println("ERROR: You need to specify a DAX file as input.");
            System.exit(1);
        } else {
            File dax = new File(args[opts.getOptind()]);
            if (dax.exists() && dax.canRead()) {
                me.setTimestamp(new Date(dax.lastModified()));
            } else {
                System.err.println("ERROR: Unable to read dax file " + dax);
                System.exit(1);
            }
        }

        DAXParser parser = new DAXParser(System.getProperty("vds.schema.dax"));
        parser.setCallback(me);
        if (!parser.parse(args[opts.getOptind()])) System.exit(42);

        me.showFinals();
    }
}
