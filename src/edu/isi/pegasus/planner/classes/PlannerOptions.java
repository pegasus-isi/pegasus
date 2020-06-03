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
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Currently;
import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Holds the information about thevarious options which user specifies to the Concrete Planner at
 * runtime.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class PlannerOptions extends Data implements Cloneable {

    /** The default logging level. */
    public static final int DEFAULT_LOGGING_LEVEL = LogManager.WARNING_MESSAGE_LEVEL;

    /** The value of number of rescue retries if no value is specified */
    public static final int DEFAULT_NUMBER_OF_RESCUE_TRIES = 999;

    /** The various cleanup options supported by the planner */
    public enum CLEANUP_OPTIONS {
        none,
        inplace,
        leaf,
        constraint
    };

    /** The base submit directory. */
    private String mBaseDir;

    /** The relative directory for the submit directory and remote execution directory */
    private String mRelativeDir;

    /**
     * The relative directory for the submit directory. Overrides relative dir if both are
     * specified.
     */
    private String mRelativeSubmitDir;

    /**
     * This is the directory where the submit files are generated on the submit host (the site
     * running the concrete planner).
     */
    // private String mSubmitFileDir ;

    /**
     * The dax file which contains the abstract dag. This dax is created by the Abstract Planner
     * using the gendax command.
     */
    private String mDAXFile;

    /** List of execution pools on which the user wants the Dag to be executed. */
    private Set<String> mExecSites;

    /** The output pool on which the data products are needed to be transferred to. */
    private Set<String> mOutputSites;

    /**
     * Set of cache files that need to be used, to determine the location of the transiency files.
     */
    private Set<String> mCacheFiles;

    /**
     * Set of replica catalog files that are inherited by a planning instance. Locations in this
     * file have a lower priority than the file locations mentioned in the DAX Replica Store
     */
    private Set<String> mInheritedRCFiles;

    /**
     * If specified, then it submits to the underlying CondorG using the kickstart-Condorscript
     * provided.
     */
    private boolean mSubmit;

    /** The force option to make a build dag from scratch. Leads to no reduction of dag occuring. */
    private boolean mForce;

    /** An enum tracking what type of cleanup needs to be done. */
    private CLEANUP_OPTIONS mCleanup;

    /** To Display help or not. */
    private boolean mDisplayHelp;

    /** Denotes the logging level that is to be used for logging the messages. */
    private int mLoggingLevel;

    /**
     * Whether to create a random directory in the execution directory that is determined from the
     * exec mount point in the pool configuration file. This forces Pegasus to do runs in a unique
     * random directory just below the execution mount point at a remote pool.
     */
    private boolean mGenRandomDir;

    /** The megadag generation mode. */
    private String mMegadag;

    /**
     * The list of VDS properties set at runtime by the user on commandline. It is a list of <code>
     * NameValue</code> pairs, with name as vds property name and value as the corresponding value.
     */
    private List mVDSProps;

    /** Denotes what type of clustering needs to be done */
    private String mClusterer;

    /**
     * If the genRandomDir flag is set, then this contains the name of the random directory. Else it
     * can be a null or an empty string.
     */
    private String mRandomDirName;

    /** Designates if the optional argument to the random directory was given. */
    private boolean mOptArg;

    /**
     * The basename prefix that is to be given to the per workflow file, like the log file, the .dag
     * file, the .cache files etc.
     */
    private String mBasenamePrefix;

    /** The prefix that is to be applied while constructing job names. */
    private String mJobPrefix;

    /**
     * A boolean indicating whether the planner invocation is part of a larger deferred planning
     * run.
     */
    private boolean mDeferredRun;

    /** The VOGroup to which the user belongs to. */
    private String mVOGroup;

    /** Stores the time at which the planning process started. */
    private Date mDate;

    /** Stores the type of partitioning to be done. */
    private String mPartitioningType;

    /** A boolean storing whether to sanitize paths or not */
    private boolean mSanitizePath;

    /** The numer of rescue's to try before replanning. */
    private int mNumOfRescueTries;

    /**
     * The properties container for properties specified on the commandline in the DAX dax elements.
     */
    private Properties mProperties;

    /** The options that need to be passed forward to pegasus-run. */
    private List<NameValue> mForwardOptions;

    /** The set of non standard java options that need to be passed to the JVM */
    private Set<String> mNonStandardJavaOptions;

    /** Returns the force replan option */
    private boolean mForceReplan;

    /** The argument string with which the planner was invoked. */
    private String mOriginalArgumentString;

    /** A map that maps an execution site to a staging site. */
    private Map<String, String> mStagingSitesMap;

    /** the input directory */
    private Set<String> mInputDirs;

    /** the output directory */
    private String mOutputDir;

    /** The conf option passed to the planner pointing to the properties file. */
    private String mConfFile;

    /** Default Constructor. */
    public PlannerOptions() {
        //        mSubmitFileDir    = ".";
        mBaseDir = ".";
        mRelativeDir = null;
        mRelativeSubmitDir = null;
        mDAXFile = null;
        mExecSites = new java.util.HashSet();
        mCacheFiles = new java.util.HashSet();
        mInheritedRCFiles = new java.util.HashSet();
        mNonStandardJavaOptions = new java.util.HashSet();
        mForwardOptions = new java.util.LinkedList<NameValue>();
        mOutputSites = new java.util.HashSet();
        mDisplayHelp = false;
        mLoggingLevel = DEFAULT_LOGGING_LEVEL;
        mForce = false;
        mSubmit = false;
        mGenRandomDir = false;
        mRandomDirName = null;
        mOptArg = false;
        mMegadag = null;
        mVDSProps = null;
        mClusterer = null;
        mBasenamePrefix = null;
        mCleanup = null;
        mVOGroup = "pegasus";
        mDeferredRun = false;
        mDate = new Date();
        mPartitioningType = null;
        mSanitizePath = true;
        mJobPrefix = null;
        mNumOfRescueTries = DEFAULT_NUMBER_OF_RESCUE_TRIES;
        mProperties = new Properties();
        mForceReplan = false;
        mOriginalArgumentString = null;
        mStagingSitesMap = new HashMap<String, String>();
        mInputDirs = new LinkedHashSet<String>();
        mOutputDir = null;
        mConfFile = null;
    }

    /**
     * Returns the cache files.
     *
     * @return Set of fully qualified paths to the cache files.
     */
    public Set<String> getCacheFiles() {
        return mCacheFiles;
    }

    /**
     * Returns the inherited rc files.
     *
     * @return Set of fully qualified paths to the cache files.
     */
    public Set<String> getInheritedRCFiles() {
        return mInheritedRCFiles;
    }

    /**
     * Returns whether to do clustering or not.
     *
     * @return boolean
     */
    //    public boolean clusteringSet(){
    //        return mCluster;
    //    }

    /**
     * Returns the clustering technique to be used for clustering.
     *
     * @return the value of clustering technique if set, else null
     */
    public String getClusteringTechnique() {
        return mClusterer;
    }

    /**
     * Returns the basename prefix for the per workflow files that are to be generated by the
     * planner.
     *
     * @return the basename if set, else null.
     */
    public String getBasenamePrefix() {
        return mBasenamePrefix;
    }

    /**
     * Returns the job prefix to be used while constructing the job names.
     *
     * @return the job prefix if set, else null.
     */
    public String getJobnamePrefix() {
        return mJobPrefix;
    }

    /**
     * Returns the path to the dax file being used by the planner.
     *
     * @return path to DAX file.
     */
    public String getDAX() {
        return mDAXFile;
    }

    /**
     * Returns the names of the execution sites where the concrete workflow can be run.
     *
     * @return <code>Set</code> of execution site names.
     */
    public Collection<String> getExecutionSites() {
        return mExecSites;
    }

    /**
     * Returns the force option set for the planner.
     *
     * @return the boolean value indicating the force option.
     */
    public boolean getForce() {
        return mForce;
    }

    /**
     * Returns the force replan option
     *
     * @return boolean
     */
    public boolean getForceReplan() {
        return mForceReplan;
    }

    /**
     * Returns the option indicating whether to do cleanup or not.
     *
     * @return the cleanup strategy to be used
     */
    public CLEANUP_OPTIONS getCleanup() {
        return mCleanup;
    }

    /**
     * Returns the time at which planning started in a ISO 8601 format.
     *
     * @param extendedFormat will use the extended ISO 8601 format which separates the different
     *     timestamp items. If false, the basic format will be used. In UTC and basic format, the
     *     'T' separator will be omitted.
     * @return String
     */
    public String getDateTime(boolean extendedFormat) {

        StringBuffer sb = new StringBuffer();
        sb.append(Currently.iso8601(false, extendedFormat, false, mDate));
        return sb.toString();
    }

    /**
     * Returns whether to display or not.
     *
     * @return help boolean value.
     */
    public boolean getHelp() {
        return mDisplayHelp;
    }

    /** Increments the logging level by 1. */
    public void incrementLogging() {
        mLoggingLevel++;
    }

    /** Deccrements the logging level by 1. */
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
     * Returns the megadag generation option .
     *
     * @return the mode if mode is set else null
     */
    public String getMegaDAGMode() {
        return this.mMegadag;
    }

    /**
     * Returns the input directory.
     *
     * @return the input directory for the workflow
     */
    public Set<String> getInputDirectories() {
        return this.mInputDirs;
    }

    /**
     * Returns the output directory
     *
     * @return the output directory for the workflow
     */
    public String getOutputDirectory() {
        return this.mOutputDir;
    }

    /**
     * Returns the names of the execution sites where the concrete workflow can be run.
     *
     * @return <code>Set</code> of output site names.
     */
    public Collection<String> getOutputSites() {
        return mOutputSites;
    }

    /**
     * Returns whether to generate a random directory or not.
     *
     * @return boolean
     */
    public boolean generateRandomDirectory() {
        return mGenRandomDir;
    }

    /**
     * Returns the random directory option.
     *
     * @return the directory name null if not set.
     */
    public String getRandomDir() {
        return mRandomDirName;
    }

    /**
     * Returns the name of the random directory, only if the generate Random Dir flag is set. Else
     * it returns null.
     */
    public String getRandomDirName() {
        if (this.generateRandomDirectory()) {
            return this.getRandomDir();
        }
        return null;
    }

    /**
     * Returns the argument string of how planner was invoked.
     *
     * @return the arguments with which the planner was invoked.
     */
    public String getOriginalArgString() {
        return this.mOriginalArgumentString;
    }

    /**
     * Sets the argument string of how planner was invoked. This function just stores the arguments
     * as a String internally.
     *
     * @param args the arguments with which the planner was invoked.
     */
    public void setOriginalArgString(String[] args) {

        StringBuffer originalArgs = new StringBuffer();
        for (int i = 0; i < args.length; i++) {
            originalArgs.append(args[i]).append(" ");
        }
        this.mOriginalArgumentString = originalArgs.toString();
    }

    /**
     * Sets a property passed on the command line.
     *
     * @param optarg key=value property specification
     */
    public void setProperty(String optarg) {
        String[] args = optarg.split("=");
        if (args.length != 2) {
            throw new RuntimeException(
                    "Wrong format for property specification on command line" + optarg);
        }
        mProperties.setProperty(args[0], args[1]);
    }

    /**
     * Returns whether to submit the workflow or not.
     *
     * @return boolean indicating whether to submit or not.
     */
    public boolean submitToScheduler() {
        return mSubmit;
    }

    /**
     * Returns the VDS properties that were set by the user.
     *
     * @return List of <code>NameValue</code> objects each corresponding to a property key and
     *     value.
     */
    public List getVDSProperties() {
        return mVDSProps;
    }

    /**
     * Returns the VO Group to which the user belongs
     *
     * @return VOGroup
     */
    public String getVOGroup() {
        return mVOGroup;
    }

    /**
     * Returns the base submit directory
     *
     * @return the path to the directory.
     */
    public String getBaseSubmitDirectory() {
        return mBaseDir;
    }

    /**
     * Returns the relative directory.
     *
     * @return the relative directory
     */
    public String getRelativeDirectory() {
        return mRelativeDir;

        //        return ( mBaseDir == null ) ?
        //                 mSubmitFileDir:
        //                 mSubmitFileDir.substring( mBaseDir.length() );
    }

    /**
     * Returns the relative submit directory option.
     *
     * @return the relative submit directory option if specified else null
     */
    public String getRelativeSubmitDirectoryOption() {
        return mRelativeSubmitDir;
    }

    /**
     * Returns the relative submit directory.
     *
     * @return the relative submit directory if specified else the relative dir.
     */
    public String getRelativeSubmitDirectory() {
        return (mRelativeSubmitDir == null)
                ? mRelativeDir
                : // pick the relative dir
                mRelativeSubmitDir; // pick the relative submit directory
    }

    /**
     * Returns the path to the directory where the submit files are to be generated. The relative
     * submit directory if specified overrides the relative directory.
     *
     * @return the path to the directory.
     */
    public String getSubmitDirectory() {
        String relative =
                (mRelativeSubmitDir == null)
                        ? mRelativeDir
                        : // pick the relative dir
                        mRelativeSubmitDir; // pick the relative submit directory

        if (mSanitizePath) {
            return (relative == null)
                    ? new File(mBaseDir).getAbsolutePath()
                    : new File(mBaseDir, relative).getAbsolutePath();
        } else {
            return (relative == null) ? mBaseDir : new File(mBaseDir, relative).getPath();
        }
    }

    /**
     * Returns the property file pointed to by the --conf option passed to the planner
     *
     * @return the conf option if passed, else null
     */
    public String getConfFile() {
        return mConfFile;
    }

    /**
     * Sets the basename prefix for the per workflow files.
     *
     * @param prefix the prefix to be set.
     */
    public void setBasenamePrefix(String prefix) {
        mBasenamePrefix = prefix;
    }

    /**
     * Sets the job prefix to be used while constructing the job names.
     *
     * @param prefix the job prefix .
     */
    public void setJobnamePrefix(String prefix) {
        mJobPrefix = prefix;
    }

    /**
     * Sets the flag to denote that the optional argument for the random was specified.
     *
     * @param value boolean indicating whether the optional argument was given or not.
     */
    public void setOptionalArg(boolean value) {
        this.mOptArg = value;
    }

    /**
     * Returns the flag to denote whether the optional argument for the random was specified or not.
     *
     * @return boolean indicating whether the optional argument was supplied or not.
     */
    public boolean optionalArgSet() {
        return this.mOptArg;
    }

    /**
     * Sets the partitioning type in case of partition and plan.
     *
     * @param type the type of partitioning technique
     */
    public void setPartitioningType(String type) {
        mPartitioningType = type;
    }

    /**
     * Returns the partitioning type in case of partition and plan.
     *
     * @return the type of partitioning technique
     */
    public String getPartitioningType() {
        return mPartitioningType;
    }

    /**
     * Sets the flag to denote that the run is part of a larger deferred run.
     *
     * @param value the value
     */
    public void setPartOfDeferredRun(boolean value) {
        mDeferredRun = value;
    }

    /**
     * Returns a boolean indicating whether this invocation is part of a deferred execution or not.
     *
     * @return boolean
     */
    public boolean partOfDeferredRun() {
        return mDeferredRun;
    }

    /**
     * Sets the flag denoting whether to sanitize path or not.
     *
     * @param value the value to set
     */
    public void setSanitizePath(boolean value) {
        mSanitizePath = value;
    }

    /**
     * Returns whether to sanitize paths or not. Internal method only.
     *
     * @return boolean
     */
    /* protected boolean sanitizePath(){
        return mSanitizePath;
    }
    */

    /**
     * Sets the caches files. If cache files have been already specified it adds to the existing set
     * of files. It also sanitizes the paths. Tries to resolve the path, if the path given is
     * relative instead of absolute.
     *
     * @param cacheList comma separated list of cache files.
     */
    public void setCacheFiles(String cacheList) {
        this.setCacheFiles(this.generateSet(cacheList));
    }

    /**
     * Sets the caches files. If cache files have been already specified it adds to the existing set
     * of files. It also sanitizes the paths. Tries to resolve the path, if the path given is
     * relative instead of absolute.
     *
     * @param files the set of fully qualified paths to the cache files.
     */
    public void setCacheFiles(Set files) {
        // use the existing set if present
        if (mCacheFiles == null) {
            mCacheFiles = new HashSet();
        }

        // traverse through each file in the set, and
        // sanitize path along the way.
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            mCacheFiles.add(this.sanitizePath((String) it.next()));
        }
    }

    /**
     * Sets the inherited RC Files. If RC files have been already specified it adds to the existing
     * set of files. It also sanitizes the paths. Tries to resolve the path, if the path given is
     * relative instead of absolute.
     *
     * @param l comma separated list of cache files.
     */
    public void setInheritedRCFiles(String list) {
        this.setInheritedRCFiles(this.generateSet(list));
    }

    /**
     * Sets the inherited RC Files. If RC files have been already specified it adds to the existing
     * set of files. It also sanitizes the paths. Tries to resolve the path, if the path given is
     * relative instead of absolute.
     *
     * @param files the set of fully qualified paths to the cache files.
     */
    public void setInheritedRCFiles(Set files) {
        // use the existing set if present
        if (this.mInheritedRCFiles == null) {
            mInheritedRCFiles = new HashSet();
        }

        // traverse through each file in the set, and
        // sanitize path along the way.
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            mInheritedRCFiles.add(this.sanitizePath((String) it.next()));
        }
    }

    /**
     * Sets the clustering option.
     *
     * @param value the value to set.
     */
    public void setClusteringTechnique(String value) {
        mClusterer = value;
    }

    /**
     * Sets the DAX that has to be worked on by the planner.
     *
     * @param dax the path to the DAX file.
     */
    public void setDAX(String dax) {
        dax = sanitizePath(dax);
        mDAXFile = dax;
    }

    /**
     * Sets the names of the execution sites where the concrete workflow can be run.
     *
     * @param siteList comma separated list of sites.
     */
    public void setExecutionSites(String siteList) {
        mExecSites = this.generateSet(siteList);
    }

    /**
     * Sets the names of the execution sites where the concrete workflow can be run.
     *
     * @param sites <code>Collection</code> of execution site names.
     */
    public void setExecutionSites(Collection sites) {
        mExecSites = new HashSet(sites);
    }

    /**
     * Sets the force option for the planner.
     *
     * @param force boolean value.
     */
    public void setForce(boolean force) {
        mForce = force;
    }

    /**
     * Sets the force replan option
     *
     * @param force the boolean value
     */
    public void setForceReplan(boolean force) {
        mForceReplan = force;
    }

    /**
     * Parses the argument in form of option=[value] and adds to the options that are to be passed
     * ahead to pegasus-run.
     *
     * @param argument the argument to be passed.
     */
    public void addToForwardOptions(String argument) {
        // split on =
        String[] arr = argument.split("=");
        NameValue nv = new NameValue();
        nv.setKey(arr[0]);
        if (arr.length == 2) {
            // set the value
            nv.setValue(arr[1]);
        }
        this.mForwardOptions.add(nv);
    }

    /**
     * Returns the forward options set
     *
     * @return List<NameValue> containing the option and the value.
     */
    public List<NameValue> getForwardOptions() {
        return this.mForwardOptions;
    }

    /**
     * Sets the cleanup option for the planner.
     *
     * @param cleanup the cleanup option
     */
    public void setCleanup(String cleanup) {
        this.setCleanup(CLEANUP_OPTIONS.valueOf(cleanup));
    }

    /**
     * Sets the cleanup option for the planner.
     *
     * @param cleanup the cleanup option
     */
    public void setCleanup(CLEANUP_OPTIONS cleanup) {
        mCleanup = cleanup;
    }

    /**
     * Sets the help option for the planner.
     *
     * @param help boolean value.
     */
    public void setHelp(boolean help) {
        mDisplayHelp = help;
    }

    /**
     * Sets the logging level for logging of messages.
     *
     * @param level the logging level.
     */
    public void setLoggingLevel(String level) {
        mLoggingLevel =
                (level != null && level.length() > 0)
                        ?
                        // the value that was passed by the user
                        new Integer(level).intValue()
                        :
                        // by default not setting it to 0,
                        // but to 1, as --verbose is an optional
                        // argument
                        1;
    }

    /**
     * Sets the megadag generation option
     *
     * @param mode the mode.
     */
    public void setMegaDAGMode(String mode) {
        this.mMegadag = mode;
    }

    /**
     * Set the input directory.
     *
     * @param input the input directory for the workflow
     */
    public void setInputDirectories(String input) {
        Set<String> dirs = new HashSet();
        for (String dir : input.split(",")) {
            dirs.add(dir);
        }
        this.setInputDirectories(dirs);
    }

    /**
     * Set the input directory.
     *
     * @param inputs the input directories to use for the workflow
     */
    public void setInputDirectories(Set<String> inputs) {
        this.mInputDirs = new HashSet();
        for (String dir : inputs) {
            this.mInputDirs.add(sanitizePath(dir));
        }
    }

    /**
     * Set the output directory.
     *
     * @param output the input directory for the workflow
     */
    public void setOutputDirectory(String output) {
        this.mOutputDir = output;
    }

    /**
     * Sets the names of the execution sites where the concrete workflow can be run.
     *
     * @param siteList comma separated list of sites.
     */
    public void setOutputSites(String siteList) {
        mOutputSites = this.generateSet(siteList);
    }

    /**
     * Sets the output site specified by the user.
     *
     * @param site the output site.
     */
    public void addOutputSite(String site) {
        mOutputSites.add(site);
    }

    /**
     * Sets the random directory in which the jobs are run.
     *
     * @param dir the basename of the random directory.
     */
    public void setRandomDir(String dir) {
        // setting the genRandomDir option to true also
        mGenRandomDir = true;
        mRandomDirName = dir;
        if (dir != null && dir.length() > 0)
            // set the flag to denote that optional arg was given
            setOptionalArg(true);
    }

    /**
     * Returns whether to submit the workflow or not.
     *
     * @param submit boolean indicating whether to submit or not.
     */
    public void setSubmitToScheduler(boolean submit) {
        mSubmit = submit;
    }

    /**
     * Sets the path to the directory where the submit files are to be generated.
     *
     * @param dir the path to the directory.
     */
    public void setSubmitDirectory(String dir) {
        this.setSubmitDirectory(dir, null);
    }

    /**
     * Sets the path to the directory where the submit files are to be generated.
     *
     * @param dir the path to the directory.
     */
    public void setSubmitDirectory(File dir) {
        this.setSubmitDirectory(dir.getAbsolutePath(), null);
    }

    /**
     * Sets the path to the directory where the submit files are to be generated.
     *
     * @param base the path to the base directory.
     * @param relative the directory relative to the base where submit files are generated.
     */
    public void setSubmitDirectory(String base, String relative) {
        base = sanitizePath(base);
        mBaseDir = base;
        // PM-1113 do the null check to ensure order of arguments
        // --dir and --relative-submit-dir is not important
        if (relative != null) {
            mRelativeSubmitDir = relative;
        }
    }

    /**
     * Sets the path to the base submitdirectory where the submit files are to be generated.
     *
     * @param base the base directory where submit files are generated.
     */
    public void setBaseSubmitDirectory(String base) {
        mBaseDir = base;
    }

    /**
     * Sets the path to the relative directory where the submit files are to be generated. The
     * submit directory can be overridden by setRelativeSubmitDirectory( String)
     *
     * @param relative the directory relative to the base where submit files are generated.
     */
    public void setRelativeDirectory(String relative) {
        mRelativeDir = relative;
    }

    /**
     * Sets the path to the directory where the submit files are to be generated.
     *
     * @param relative the directory relative to the base where submit files are generated.
     */
    public void setRelativeSubmitDirectory(String relative) {
        mRelativeSubmitDir = relative;
    }

    /**
     * Sets the VDS properties specifed by the user at the command line.
     *
     * @param properties List of <code>NameValue</code> objects.
     */
    public void setVDSProperties(List properties) {
        mVDSProps = properties;
    }

    /**
     * Set the VO Group to which the user belongs
     *
     * @param group the VOGroup
     */
    public void setVOGroup(String group) {
        mVOGroup = group;
    }

    /**
     * Sets the number of times to try for rescue dag submission.
     *
     * @param num number.
     */
    public void setNumberOfRescueTries(String num) {
        this.mNumOfRescueTries = Integer.parseInt(num);
    }

    /**
     * Sets the number of times to try for rescue dag submission.
     *
     * @param num number.
     */
    public void setNumberOfRescueTries(int num) {
        this.mNumOfRescueTries = num;
    }

    /**
     * Returns the number of times to try for rescue dag submission.
     *
     * @return number.
     */
    public int getNumberOfRescueTries() {
        return this.mNumOfRescueTries;
    }

    /**
     * Adds to the Set of non standard JAVA options that need to be passed to the JVM. The list of
     * non standard java options can be retrieved by doing java -X .
     *
     * <p>The option is always prefixed by -X internally. If mx1024m is passed, internally option
     * will be set to -Xmx1024m
     *
     * @param option the non standard option.
     */
    public void addToNonStandardJavaOptions(String option) {

        this.mNonStandardJavaOptions.add("-X" + option);
    }

    /**
     * Returns the Set of non standard java options.
     *
     * @return Set<String>
     */
    public Set<String> getNonStandardJavaOptions() {
        return this.mNonStandardJavaOptions;
    }

    /**
     * Adds to the staging sites
     *
     * @param value comma separated key=value pairs where key is execution site and value is the
     *     staging site to use for that execution site
     */
    public void addToStagingSitesMappings(String value) {
        if (value == null) {
            // do nothing
            return;
        }

        for (String kvstr : value.split(",")) {
            // kvstr  is of form key=value
            String[] kv = kvstr.split("=");
            if (kv.length == 1) {
                // add a * notation
                addToStagingSitesMappings("*", kv[0]);
            } else {
                addToStagingSitesMappings(kv[0], kv[1]);
            }
        }
    }

    /**
     * Adds to the staging sites
     *
     * @param executionSite the execution site
     * @param stagingSite the staging site.
     */
    public void addToStagingSitesMappings(String executionSite, String stagingSite) {

        this.mStagingSitesMap.put(executionSite, stagingSite);
    }

    /**
     * Returns the staging site for an execution site.
     *
     * @param executionSite the execution site
     * @return the staging site corresponding to an execution site, else null
     */
    public String getStagingSite(String executionSite) {

        return (this.mStagingSitesMap.containsKey(executionSite)
                ? this.mStagingSitesMap.get(executionSite)
                : // the mapping for the execution site
                this.mStagingSitesMap.get("*") // the value for the star site if specified
        );
    }

    /**
     * Convers the staging site mappings to comma separated list of executionsite=stagingsite
     * mappings
     *
     * @return mappings as string
     */
    protected String stagingSiteMappingToString() {
        StringBuffer sb = new StringBuffer();

        for (Map.Entry<String, String> entry : this.mStagingSitesMap.entrySet()) {
            String eSite = entry.getKey();
            String sSite = entry.getValue();
            if (eSite.equals("*")) {
                // for a star notation just add the corresponding staging site
                sb.append(sSite);
            } else {
                sb.append(eSite).append("=").append(sSite);
            }
            sb.append(",");
        }

        return sb.toString();
    }

    /**
     * Sets the property file pointed to by the --conf option passed to the planner
     *
     * @param conf the conf option if passed, else null
     */
    public void setConfFile(String conf) {
        mConfFile = conf;
    }

    /**
     * Returns the textual description of all the options that were set for the planner.
     *
     * @return the textual description.
     */
    public String toString() {
        String st =
                "\n"
                        + "\n Planner Options"
                        + "\n Argument String As Seen By Planner "
                        + this.getOriginalArgString()
                        + "\n Base Submit Directory "
                        + mBaseDir
                        + "\n SubmitFile Directory "
                        + this.getSubmitDirectory()
                        + "\n Basename Prefix      "
                        + mBasenamePrefix
                        + "\n Jobname Prefix       "
                        + mJobPrefix
                        + "\n Abstract Dag File    "
                        + mDAXFile
                        + "\n Execution Sites      "
                        + this.setToString(mExecSites, ",")
                        + "\n Staging Sites        "
                        + this.stagingSiteMappingToString()
                        + "\n Cache Files          "
                        + this.setToString(mCacheFiles, ",")
                        + "\n Inherited RC Files   "
                        + this.setToString(mInheritedRCFiles, ",")
                        + "\n Input Directory      "
                        + this.setToString(mInputDirs, ",")
                        + "\n Output Directory     "
                        + this.mOutputDir
                        + "\n Output Sites          "
                        + this.setToString(mOutputSites, ",")
                        + "\n Submit to HTCondor Schedd    "
                        + mSubmit
                        + "\n Display Help         "
                        + mDisplayHelp
                        + "\n Logging Level        "
                        + mLoggingLevel
                        + "\n Force Option         "
                        + mForce
                        + "\n Force Replan         "
                        + mForceReplan
                        + "\n Cleanup within wf    "
                        + mCleanup
                        + "\n Create Random Direct "
                        + mGenRandomDir
                        + "\n Random Direct Name   "
                        + mRandomDirName
                        + "\n Clustering Technique "
                        + mClusterer
                        + "\n Cleanup    "
                        + mCleanup
                        + "\n VO Group             "
                        + mVOGroup
                        + "\n Rescue Tries         "
                        + mNumOfRescueTries
                        + "\n VDS Properties       "
                        + mVDSProps
                        + "\n Non Standard JVM Options "
                        + this.mNonStandardJavaOptions;
        return st;
    }

    /**
     * Generates the argument string corresponding to these options that can be used to invoke
     * Pegasus. During its generation it ignores the dax and pdax options as they are specified
     * elsewhere.
     *
     * @return all the options in a String separated by whitespace.
     */
    public String toOptions() {
        StringBuffer sb = new StringBuffer();

        // the conf option if set always has to be the first one!
        if (mConfFile != null) {
            sb.append(" --conf ").append(mConfFile);
        }

        // the submit file dir
        //        if( mSubmitFileDir != null){ sb.append(" --dir ").append(mSubmitFileDir);}
        // confirm how this plays in deferred planning. not clear. Karan Oct 31 2007
        sb.append(" --dir ").append(this.getBaseSubmitDirectory());

        if (mRelativeDir != null) {
            sb.append(" --relative-dir ").append(this.getRelativeDirectory());
        }

        if (mRelativeSubmitDir != null) {
            sb.append(" --relative-submit-dir ").append(mRelativeSubmitDir);
        }

        // the basename prefix
        if (mBasenamePrefix != null) {
            sb.append(" --basename ").append(mBasenamePrefix);
        }

        // the jobname prefix
        if (mJobPrefix != null) {
            sb.append(" --job-prefix ").append(mJobPrefix);
        }

        if (!mExecSites.isEmpty()) {
            sb.append(" --sites ");
            // generate the comma separated string
            // for the execution pools
            sb.append(setToString(mExecSites, ","));
        }

        if (!this.mStagingSitesMap.isEmpty()) {
            sb.append(" --staging-site ").append(this.stagingSiteMappingToString());
        }

        // cache files
        if (!mCacheFiles.isEmpty()) {
            sb.append(" --cache ").append(setToString(mCacheFiles, ","));
        }

        // inherited rc files
        if (!mInheritedRCFiles.isEmpty()) {
            sb.append(" --inherited-rc-files ").append(setToString(mInheritedRCFiles, ","));
        }

        // collapse option
        if (mClusterer != null) {
            sb.append(" --cluster ").append(mClusterer);
        }

        // specify the input directory
        if (!this.mInputDirs.isEmpty()) {
            sb.append(" --input-dir ").append(setToString(this.mInputDirs, ","));
        }

        // specify the output directory
        if (this.mOutputDir != null) {
            sb.append(" --output-dir ").append(this.mOutputDir);
        }
        // specify the output site
        if (!mOutputSites.isEmpty()) {
            sb.append(" --output-site ");
            // generate the comma separated string
            // for the execution pools
            sb.append(setToString(mOutputSites, ","));
        }

        // the condor submit option
        if (mSubmit) {
            sb.append(" --run ");
        }

        // the force option
        if (mForce) {
            sb.append(" --force ");
        }

        // the force replan option
        if (mForceReplan) {
            sb.append(" --force-replan ");
        }

        // the cleanup option
        if (mCleanup != null) {
            sb.append(" --cleanup ").append(mCleanup.name());
        }
        // if( !mCleanup ){ sb.append(" --nocleanup "); }

        // the verbose option
        for (int i = PlannerOptions.DEFAULT_LOGGING_LEVEL; i < getLoggingLevel(); i++) {
            sb.append(" --verbose ");
        }
        // add any quiet logging options if required
        for (int i = getLoggingLevel(); i < PlannerOptions.DEFAULT_LOGGING_LEVEL; i++) {
            sb.append(" --quiet ");
        }

        // the deferred run option
        if (mDeferredRun) {
            sb.append(" --deferred ");
        }

        // the random directory
        if (mGenRandomDir) {
            // an optional argument
            sb.append(" --randomdir");
            if (this.getRandomDir() == null) {
                // no argument to be given
                sb.append(" ");
            } else {
                // add the optional argument
                sb.append("=").append(getRandomDir());
            }
        }

        // specify the megadag option if set
        if (mMegadag != null) {
            sb.append(" --megadag ").append(mMegadag);
        }

        // specify the vogroup
        sb.append(" --group ").append(mVOGroup);

        // specify the number of times to try rescue
        // only if it does not match the default value!
        if (this.getNumberOfRescueTries() != PlannerOptions.DEFAULT_NUMBER_OF_RESCUE_TRIES) {
            sb.append(" --rescue ").append(this.getNumberOfRescueTries());
        }

        // help option
        if (mDisplayHelp) {
            sb.append(" --help ");
        }

        return sb.toString();
    }

    /**
     * Converts the vds properties that need to be passed to the jvm as an option.
     *
     * @return the jvm options as String.
     */
    public String toJVMOptions() {
        StringBuffer sb = new StringBuffer();

        if (mVDSProps != null) {
            for (Iterator it = mVDSProps.iterator(); it.hasNext(); ) {
                NameValue nv = (NameValue) it.next();
                sb.append(" -D").append(nv.getKey()).append("=").append(nv.getValue());
            }
        }

        // add all the properties specified in the dax elements
        for (Iterator<Object> it = mProperties.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            sb.append(" -D").append(key).append("=").append(mProperties.getProperty(key));
        }

        // pass on all the -X options to jvm
        for (Iterator<String> it = this.mNonStandardJavaOptions.iterator(); it.hasNext(); ) {
            sb.append(" ").append(it.next());
        }

        return sb.toString();
    }

    /**
     * Returns the complete options string that is used to invoke pegasus
     *
     * @return the options as string.
     */
    public String getCompleteOptions() {
        StringBuffer sb = new StringBuffer();
        sb.
                /*append( this.toJVMOptions() ).append( " " ).*/
                append(this.toOptions())
                .append("--dax")
                .append(" ")
                .append(this.getDAX());

        return sb.toString();
    }

    /**
     * Clones a Set.
     *
     * @param s Set
     * @return the cloned set as a HashSet
     */
    private Set cloneSet(Set s) {
        java.util.Iterator it = s.iterator();
        Set newSet = new java.util.HashSet();

        while (it.hasNext()) {
            newSet.add(it.next());
        }

        return newSet;
    }

    /**
     * Returns a new copy of the Object. The clone does not clone the internal VDS properties at the
     * moment.
     *
     * @return the cloned copy.
     */
    public Object clone() {
        PlannerOptions pOpt = null;

        try {
            pOpt = (PlannerOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            mLogger.log(
                    "Clone not implemented in the base class of " + this.getClass().getName(),
                    LogManager.WARNING_MESSAGE_LEVEL);
            // try calling the constructor directly
            pOpt = new PlannerOptions();
        }
        //        pOpt.mSubmitFileDir  = this.mSubmitFileDir;
        pOpt.mBaseDir = this.mBaseDir;
        pOpt.mRelativeDir = this.mRelativeDir;
        pOpt.mDAXFile = this.mDAXFile;
        pOpt.mExecSites = cloneSet(this.mExecSites);
        pOpt.mStagingSitesMap = new HashMap<String, String>(this.mStagingSitesMap);
        pOpt.mCacheFiles = cloneSet(this.mCacheFiles);
        pOpt.mInheritedRCFiles = cloneSet(this.mInheritedRCFiles);
        pOpt.mNonStandardJavaOptions = cloneSet(this.mNonStandardJavaOptions);
        pOpt.mInputDirs = cloneSet(this.mInputDirs);
        pOpt.mOutputDir = this.mOutputDir;
        pOpt.mOutputSites = cloneSet(this.mOutputSites);
        pOpt.mDisplayHelp = this.mDisplayHelp;
        pOpt.mLoggingLevel = this.mLoggingLevel;
        pOpt.mForce = this.mForce;
        pOpt.mForceReplan = this.mForceReplan;
        pOpt.mCleanup = this.mCleanup;
        pOpt.mSubmit = this.mSubmit;
        pOpt.mGenRandomDir = this.mGenRandomDir;
        pOpt.mOptArg = this.mOptArg;
        pOpt.mRandomDirName = this.mRandomDirName;
        pOpt.mClusterer = this.mClusterer;
        pOpt.mBasenamePrefix = this.mBasenamePrefix;
        pOpt.mJobPrefix = this.mJobPrefix;
        pOpt.mVOGroup = this.mVOGroup;
        pOpt.mDeferredRun = this.mDeferredRun;
        pOpt.mDate = (Date) this.mDate.clone();
        pOpt.mPartitioningType = this.mPartitioningType;
        pOpt.mNumOfRescueTries = this.mNumOfRescueTries;
        pOpt.mOriginalArgumentString = this.mOriginalArgumentString;
        pOpt.mConfFile = this.mConfFile;

        // a shallow clone for forward options
        pOpt.mForwardOptions = this.mForwardOptions;

        pOpt.mProperties = (Properties) this.mProperties.clone();
        // Note not cloning the vdsProps or mProperties
        pOpt.mVDSProps = null;
        return pOpt;
    }

    /**
     * Generates a Set by parsing a comma separated string.
     *
     * @param str the comma separted String.
     * @return Set containing the parsed values, in case of a null string an empty set is returned.
     */
    private Set generateSet(String str) {
        Set s = new HashSet();

        // check for null
        if (s == null) {
            return s;
        }

        for (StringTokenizer st = new StringTokenizer(str, ","); st.hasMoreElements(); ) {
            s.add(st.nextToken().trim());
        }

        return s;
    }

    /**
     * A small utility method that santizes the url, converting it from relative to absolute. In
     * case the path is relative, it uses the System property user.dir to get the current working
     * directory, from where the planner is being run.
     *
     * @param path the absolute or the relative path.
     * @return the absolute path.
     */
    private String sanitizePath(String path) {
        if (path == null) {
            return null;
        }

        if (!mSanitizePath) {
            return path;
        }

        String absPath;
        char separator = File.separatorChar;

        absPath =
                (path.indexOf(separator) == 0)
                        ?
                        // absolute path given already
                        path
                        :
                        // get the current working dir
                        System.getProperty("user.dir")
                                + separator
                                + ((path.indexOf('.') == 0)
                                        ? // path starts with a . ?
                                        ((path.indexOf(separator) == 1)
                                                ? // path starts with a ./ ?
                                                path.substring(2)
                                                : (path.length() > 1 && path.charAt(1) == '.')
                                                        ? // path starts with .. ?
                                                        path
                                                        : // keep path as it is
                                                        path.substring(path.indexOf('.') + 1))
                                        : path);

        // remove trailing separator if any
        absPath =
                (absPath.lastIndexOf(separator) == absPath.length() - 1)
                        ? absPath.substring(0, absPath.length() - 1)
                        : absPath;

        return absPath;
    }
}
