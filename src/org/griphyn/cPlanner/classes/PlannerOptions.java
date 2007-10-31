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
package org.griphyn.cPlanner.classes;

import org.griphyn.cPlanner.common.LogManager;

import java.io.File;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.griphyn.common.util.Currently;

/**
 * Holds the information about thevarious options which user specifies to
 * the Concrete Planner at runtime.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class PlannerOptions extends Data implements Cloneable{

    /**
     * The base submit directory.
     */
    private String mBaseDir;

    /**
     * This is the directory where the submit files are generated on the submit
     * host (the site running the concrete planner).
     */
    private String mSubmitFileDir ;

    /**
     * The dax file which contains the abstract dag. This dax is created by the
     * Abstract Planner using the gendax command.
     */
    private String mDAXFile;

    /**
     * The path to the pdax file that contains the partition graph.
     */
    private String mPDAXFile;

    /**
     * List of execution pools on which the user wants the Dag to be executed.
     */
    private Set mvExecPools;

    /**
     * Set of cache files that need to be used, to determine the location of the
     * transiency files.
     */
    private Set mCacheFiles;

    /**
     * The output pool on which the data products are needed to be transferred to.
     */
    private String mOutputPool;

    /**
     * If specified, then it submits to the underlying CondorG using the
     * kickstart-Condorscript provided.
     */
    private boolean mSubmit;

    /**
     * The force option to make a build dag from scratch. Leads to no reduction
     * of dag occuring.
     */
    private boolean mForce;

    /**
     * A boolean indicating whether to do cleanup or not.
     */
    private boolean mCleanup;

    /**
     * To Display help or not.
     */
    private boolean mDisplayHelp;

    /**
     * Denotes the logging level that is to be used for logging the messages.
     */
    private int mLoggingLevel;

    /**
     * Whether to create a random directory in the execution directory that is
     * determined from the exec mount point in the pool configuration file.
     * This forces Pegasus to do runs in a unique random directory just below
     * the execution mount point at a remote pool.
     */
    private boolean mGenRandomDir;

    /**
     * Whether to attempt authentication against the jobmanagers for the pools
     * specified at runtime.
     */
    private boolean mAuthenticate;

    /**
     * The megadag generation mode.
     */
    private String mMegadag;

    /**
     * The list of VDS properties set at runtime by the user on commandline.
     * It is a list of <code>NameValue</code> pairs, with name as vds property
     * name and value as the corresponding value.
     */
    private List mVDSProps;

    /**
     * Denotes what type of clustering needs to be done
     */
    private String mClusterer;

    /**
     * If the genRandomDir flag is set, then this contains the name of
     * the random directory. Else it can be a null or an empty string.
     */
    private String mRandomDirName;

    /**
     * Designates if the optional argument to the random directory was given.
     */
    private boolean mOptArg;

    /**
     * The basename prefix that is to be given to the per workflow file, like
     * the log file, the .dag file, the .cache files etc.
     */
    private String mBasenamePrefix;

    /**
     * A boolean indicating whether the planner invocation is part of a larger
     * deferred planning run.
     */
    private boolean mDeferredRun;

    /**
     * Boolean indicating whether to spawn off a monitoring process or not
     * for the workflow.
     */
    private boolean mMonitor;

    /**
     * The VOGroup to which the user belongs to.
     */
    private String mVOGroup;

    /**
     * Stores the time at which the planning process started.
     */
    private Date mDate;

    /**
     * Default Constructor.
     */
    public PlannerOptions(){
        mSubmitFileDir    = ".";
        mBaseDir          = ".";
        mDAXFile          = null;
        mPDAXFile         = null;
        mvExecPools       = new java.util.HashSet();
        mCacheFiles       = new java.util.HashSet();
        mOutputPool       = null;
        mDisplayHelp      = false;
        mLoggingLevel     = 0;
        mForce            = false;
        mSubmit           = false;
        mGenRandomDir     = false;
        mRandomDirName    = null;
        mOptArg           = false;
        mAuthenticate     = false;
        mMegadag          = null;
        mVDSProps         = null;
        mClusterer        = null;
        mBasenamePrefix   = null;
        mMonitor          = false;
        mCleanup          = true;
        mVOGroup          = "pegasus";
        mDeferredRun      = false;
        mDate             = new Date();
    }

    /**
     * Returns the authenticate option for the planner.
     *
     * @return boolean indicating if it was set or not.
     */
    public boolean authenticationSet(){
        return mAuthenticate;
    }

    /**
     * Returns the cache files.
     *
     * @return Set of fully qualified paths to the cache files.
     *
     */
    public Set getCacheFiles(){
        return mCacheFiles;
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
    public String getClusteringTechnique(){
        return mClusterer;
    }

    /**
     * Returns the basename prefix for the per workflow files that are to be
     * generated by the planner.
     *
     * @return the basename if set, else null.
     */
    public String getBasenamePrefix(){
        return mBasenamePrefix;
    }

    /**
     * Returns the path to the dax file being used by the planner.
     *
     * @return path to DAX file.
     */
    public String getDAX(){
        return mDAXFile;
    }

    /**
     * Returns the names of the execution sites where the concrete workflow can
     * be run.
     *
     * @return  <code>Set</code> of execution site names.
     */
    public Collection getExecutionSites(){
        return mvExecPools;
    }

    /**
     * Returns the force option set for the planner.
     *
     * @return the boolean value indicating the force option.
     */
    public boolean getForce(){
        return mForce;
    }

    /**
     * Returns the option indicating whether to do cleanup or not.
     *
     * @return the boolean value indicating the cleanup option.
     */
    public boolean getCleanup(){
        return mCleanup;
    }


    /**
     * Returns the time at which planning started in a ISO 8601 format.
     *
     * @param extendedFormat  will use the extended ISO 8601 format which
     * separates the different timestamp items. If false, the basic
     * format will be used. In UTC and basic format, the 'T' separator
     * will be omitted.
     *
     * @return String
     */
    public String getDateTime( boolean extendedFormat ){

        StringBuffer sb = new StringBuffer();
        sb.append( Currently.iso8601( false, extendedFormat, false,
                                      mDate )
                  );
        return sb.toString();

    }


    /**
     * Returns whether to display or not.
     *
     * @return help boolean value.
     */
    public boolean getHelp(){
        return mDisplayHelp;
    }

    /**
     * Increments the logging level by 1.
     */
    public void incrementLogging(){
        mLoggingLevel++;
    }

    /**
     * Returns the logging level.
     *
     * @return  the logging level.
     */
    public int getLoggingLevel(){
        return mLoggingLevel;
    }

    /**
     * Returns the megadag generation option .
     *
     * @return the mode if mode is set
     *         else null
     */
    public String getMegaDAGMode(){
        return this.mMegadag;
    }

    /**
     * Returns the output site where to stage the output .
     *
     * @return the output site.
     */
    public String getOutputSite(){
        return mOutputPool;
    }

    /**
     * Returns the path to the PDAX file being used by the planner.
     *
     * @return path to PDAX file.
     */
    public String getPDAX(){
        return mPDAXFile;
    }

    /**
     * Returns whether to generate a random directory or not.
     *
     * @return boolean
     */
    public boolean generateRandomDirectory(){
        return mGenRandomDir;
    }

    /**
     * Returns the random directory option.
     *
     * @return  the directory name
     *          null if not set.
     */
    public String getRandomDir(){
        return mRandomDirName;
    }

    /**
     * Returns whether to submit the workflow or not.
     *
     * @return boolean indicating whether to submit or not.
     */
    public boolean submitToScheduler(){
        return mSubmit;
    }

    /**
     * Returns the VDS properties that were set by the user.
     *
     * @return  List of <code>NameValue</code> objects each corresponding to a
     *          property key and value.
     */
    public List getVDSProperties(){
        return mVDSProps;
    }

    /**
     * Returns the VO Group to which the user belongs
     *
     * @return VOGroup
     */
    public String getVOGroup( ){
        return mVOGroup;
    }

    /**
     * Returns the base submit directory
     *
     * @return the path to the directory.
     */
    public String getBaseSubmitDirectory(){
        return mBaseDir;
    }

    /**
     * Returns the relative submit directory.
     *
     * @return the relative submit directory
     */
    public String getRelativeSubmitDirectory(){
        return ( mBaseDir == null ) ?
                 mSubmitFileDir:
                 mSubmitFileDir.substring( mBaseDir.length() );
    }



    /**
     * Returns the path to the directory where the submit files are to be
     * generated.
     *
     * @return the path to the directory.
     */
    public String getSubmitDirectory(){
        return mSubmitFileDir;
    }

    /**
     * Sets the authenticate flag to the value passed.
     *
     * @param value  boolean value passed.
     */
    public void setAuthentication(boolean value){
        mAuthenticate = value;
    }

    /**
     * Sets the basename prefix for the per workflow files.
     *
     * @param prefix  the prefix to be set.
     */
    public void setBasenamePrefix(String prefix){
        mBasenamePrefix = prefix;
    }


    /**
     * Sets the flag to denote that the optional argument for the random was
     * specified.
     *
     * @param value boolean indicating whether the optional argument was given
     *              or not.
     */
    public void setOptionalArg(boolean value){
        this.mOptArg = value;
    }

    /**
     * Returns the flag to denote whether the optional argument for the random was
     * specified or not.
     *
     * @return boolean indicating whether the optional argument was supplied or not.
     */
    public boolean optionalArgSet(){
        return this.mOptArg;
    }


    /**
     * Sets the flag to denote whether we want to monitor the workflow or not.
     *
     * @param value boolean.
     */
    public void setMonitoring(boolean value){
        this.mMonitor = value;
    }

    /**
     * Returns boolean indicating whether we want to monitor or not.
     *
     * @return boolean indicating whether monitoring was set or not.
     */
    public boolean monitorWorkflow(){
        return this.mMonitor;
    }


    /**
     * Sets the flag to denote that the run is part of a larger deferred run.
     *
     * @param value the value
     */
    public void setPartOfDeferredRun( boolean value ){
        mDeferredRun = value;
    }

    /**
     * Returns a boolean indicating whether this invocation is part of a
     * deferred execution or not.
     *
     * @return boolean
     */
    public boolean partOfDeferredRun( ){
        return mDeferredRun;
    }

    /**
     * Sets the caches files. If cache files have been already specified it
     * adds to the existing set of files. It also sanitizes the paths. Tries
     * to resolve the path, if the path given is relative instead of absolute.
     *
     * @param cacheList  comma separated list of cache files.
     */
    public void setCacheFiles( String cacheList ){
        this.setCacheFiles( this.generateSet(cacheList) );
    }


    /**
     * Sets the caches files. If cache files have been already specified it
     * adds to the existing set of files. It also sanitizes the paths. Tries
     * to resolve the path, if the path given is relative instead of absolute.
     *
     * @param files  the set of fully qualified paths to the cache files.
     *
     */
    public void setCacheFiles(Set files){
        //use the existing set if present
        if (mCacheFiles == null ) { mCacheFiles = new HashSet(); }

        //traverse through each file in the set, and
        //sanitize path along the way.
        for ( Iterator it = files.iterator(); it.hasNext(); ){
            mCacheFiles.add( this.sanitizePath( (String)it.next() ) );
        }
    }


    /**
     * Sets the clustering option.
     *
     * @param value  the value to set.
     */
    public void setClusteringTechnique( String value ){
        mClusterer = value;
    }

    /**
     * Sets the DAX that has to be worked on by the planner.
     *
     * @param dax  the path to the DAX file.
     */
    public void setDAX(String dax){
        dax = sanitizePath(dax);
        mDAXFile = dax;
    }

    /**
     * Sets the names of the execution sites where the concrete workflow can
     * be run.
     *
     * @param siteList  comma separated list of sites.
     */
    public void setExecutionSites(String siteList){

        mvExecPools = this.generateSet( siteList );
    }


    /**
     * Sets the names of the execution sites where the concrete workflow can
     * be run.
     *
     * @param sites  <code>Collection</code> of execution site names.
     */
    public void setExecutionSites(Collection sites){
        mvExecPools = (Set)sites;
    }


    /**
     * Sets the force option for the planner.
     *
     * @param force  boolean value.
     */
    public void setForce(boolean force){
        mForce = force;
    }

    /**
     * Sets the cleanup option for the planner.
     *
     * @param cleanup  boolean value.
     */
    public void setCleanup( boolean cleanup ){
        mCleanup = cleanup;
    }


    /**
     * Sets the help option for the planner.
     *
     * @param help boolean value.
     */
    public void setHelp(boolean help){
        mDisplayHelp = help;
    }

    /**
     * Sets the logging level for logging of messages.
     *
     * @param level the logging level.
     */
    public void setLoggingLevel(String level){
         mLoggingLevel = (level != null && level.length() > 0)?
                       //the value that was passed by the user
                       new Integer(level).intValue():
                       //by default not setting it to 0,
                       //but to 1, as --verbose is an optional
                       //argument
                       1;
    }

    /**
     * Sets the megadag generation option
     *
     * @param mode the mode.
     */
    public void setMegaDAGMode(String mode){
        this.mMegadag = mode;
    }

    /**
     * Sets the PDAX that has to be worked on by the planner.
     *
     * @param pdax  the path to the PDAX file.
     */
    public void setPDAX(String pdax){
        pdax = sanitizePath(pdax);
        mPDAXFile = pdax;
    }

    /**
     * Sets the output site specified by the user.
     *
     * @param site the output site.
     */
    public void setOutputSite(String site){
        mOutputPool = site;
    }

    /**
     * Sets the random directory in which the jobs are run.
     *
     * @param dir  the basename of the random directory.
     */
    public void setRandomDir(String dir){
        //setting the genRandomDir option to true also
        mGenRandomDir  = true;
        mRandomDirName = dir;
        if(dir != null && dir.length() > 0)
            //set the flag to denote that optional arg was given
            setOptionalArg(true);
    }

    /**
     * Returns whether to submit the workflow or not.
     *
     * @param submit boolean indicating whether to submit or not.
     */
    public void setSubmitToScheduler(boolean submit){
        mSubmit = submit;
    }


    /**
     * Sets the path to the directory where the submit files are to be
     * generated.
     *
     * @param dir the path to the directory.
     */
    public void setSubmitDirectory( String dir ){
        this.setSubmitDirectory( dir, null );
    }


    /**
     * Sets the path to the directory where the submit files are to be
     * generated.
     *
     * @param dir the path to the directory.
     */
    public void setSubmitDirectory( File dir ){
        this.setSubmitDirectory( dir.getAbsolutePath() , null );
    }


    /**
     * Sets the path to the directory where the submit files are to be
     * generated.
     *
     * @param base       the path to the base directory.
     * @param relative   the directory relative to the base where submit files are generated.
     */
    public void setSubmitDirectory( String base, String relative ){
        base = sanitizePath( base );
        mSubmitFileDir = ( relative == null )?
                         new File( base ).getAbsolutePath():
                         new File( base, relative ).getAbsolutePath();
        mBaseDir  = base;
    }




    /**
     * Sets the VDS properties specifed by the user at the command line.
     *
     * @param properties  List of <code>NameValue</code> objects.
     */
    public void setVDSProperties(List properties){
        mVDSProps = properties;
    }


    /**
     * Set the VO Group to which the user belongs
     *
     * @param group  the VOGroup
     */
    public void setVOGroup( String group ){
        mVOGroup = group;
    }


    /**
     * Returns the textual description of all the options that were set for
     * the planner.
     *
     * @return the textual description.
     */
    public String toString(){
        String st = "\n" +
                    "\n Concrete Planner Options" +
                    "\n Base Submit Directory " + mBaseDir +
                    "\n SubmitFile Directory " + mSubmitFileDir +
                    "\n Basename Prefix      " + mBasenamePrefix +
                    "\n Abstract Dag File    " + mDAXFile +
                    "\n Partition File       " + mPDAXFile +
                    "\n Execution Pools      " + this.setToString(mvExecPools,",")+
                    "\n Cache Files          " + this.setToString(mCacheFiles,",") +
                    "\n Output Pool          " + mOutputPool +
                    "\n Submit to CondorG    " + mSubmit +
                    "\n Display Help         " + mDisplayHelp +
                    "\n Logging Level        " + mLoggingLevel +
                    "\n Force Option         " + mForce +
                    "\n Cleanup within wf    " + mCleanup +
                    "\n Create Random Direct " + mGenRandomDir +
                    "\n Random Direct Name   " + mRandomDirName +
                    "\n Authenticate         " + mAuthenticate +
                    "\n Clustering Technique " + mClusterer +
                    "\n Monitor Workflow     " + mMonitor +
                    "\n VO Group             " + mVOGroup +
                    "\n VDS Properties       " + mVDSProps;
        return st;
    }

    /**
     * Generates the argument string corresponding to these options that can
     * be used to invoke Pegasus. During its generation it ignores the
     * dax and pdax options as they are specified elsewhere.
     *
     * @return all the options in a String separated by whitespace.
     */
    public String toOptions(){
        StringBuffer sb = new StringBuffer();

        //the submit file dir
        if(mSubmitFileDir != null){ sb.append(" --dir ").append(mSubmitFileDir);}

        //the basename prefix
        if(mBasenamePrefix != null){ sb.append(" --basename ").append(mBasenamePrefix);}

        if(!mvExecPools.isEmpty()){
            sb.append(" --sites ");
            //generate the comma separated string
            //for the execution pools
            sb.append(setToString(mvExecPools,","));
        }

        //cache files
        if(!mCacheFiles.isEmpty()){
            sb.append(" --cache ").append(setToString(mCacheFiles,","));
        }

        //collapse option
        if( mClusterer != null ){ sb.append(" --cluster ").append(mClusterer);}

        //specify the output pool
        if(mOutputPool != null){ sb.append(" --output ").append(mOutputPool);}

        //the condor submit option
        if(mSubmit){ sb.append(" --run "); }

        //the force option
        if(mForce){ sb.append(" --force "); }


        //the cleanup option
        if( !mCleanup ){ sb.append(" --nocleanup "); }


        //the verbose option
        for(int i = 0; i < getLoggingLevel();i++)
            sb.append(" --verbose " );

        //the monitor option
        if( mMonitor ) { sb.append(" --monitor "); }

        //the deferred run option
        if( mDeferredRun ) { sb.append( " --deferred "); }

        //the random directory
        if(mGenRandomDir){
            //an optional argument
            sb.append(" --randomdir");
            if(this.getRandomDir() == null){
                //no argument to be given
                sb.append(" ");
            }
            else{
                //add the optional argument
                sb.append("=").append(getRandomDir());
            }
        }

        //the authenticate option
        if(mAuthenticate){  sb.append(" --authenticate"); }

        //specify the megadag option if set
        if(mMegadag != null){ sb.append(" --megadag ").append(mMegadag);}

        //specify the vogroup
        sb.append(" --group ").append( mVOGroup );

        //help option
        if(mDisplayHelp){  sb.append(" --help ");}

        return sb.toString();
    }


    /**
     * Converts the vds properties that need to be passed to the jvm as an
     * option.
     *
     * @return the jvm options as String.
     */
    public String toJVMOptions(){
        StringBuffer sb = new StringBuffer();

        Iterator it = (mVDSProps == null)?null:mVDSProps.iterator();

        if(it != null){
            while(it.hasNext()){
                NameValue nv = (NameValue)it.next();
                sb.append(" -D").append(nv.getKey()).append("=").append(nv.getValue());
            }
        }

        return sb.toString();
    }


    /**
     * Clones a Set.
     *
     * @param s Set
     *
     * @return  the cloned set as a HashSet
     */
    private Set cloneSet(Set s){
        java.util.Iterator it = s.iterator();
        Set newSet = new java.util.HashSet();

        while(it.hasNext()){
            newSet.add(it.next());
        }

        return newSet;
    }

    /**
     * Returns a new copy of the Object. The clone does not clone the internal
     * VDS properties at the moment.
     *
     * @return the cloned copy.
     */
    public Object clone(){
        PlannerOptions pOpt  = null;

        try{
            pOpt = (PlannerOptions)super.clone();
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            mLogger.log("Clone not implemented in the base class of " + this.getClass().getName(),
                        LogManager.WARNING_MESSAGE_LEVEL);
            //try calling the constructor directly
            pOpt = new PlannerOptions();
        }
        pOpt.mSubmitFileDir  = this.mSubmitFileDir;
        pOpt.mBaseDir        = this.mBaseDir;
        pOpt.mDAXFile        = this.mDAXFile;
        pOpt.mPDAXFile       = this.mPDAXFile;
        pOpt.mvExecPools     = cloneSet(this.mvExecPools);
        pOpt.mCacheFiles     = cloneSet(this.mCacheFiles);
        pOpt.mOutputPool     = this.mOutputPool;
        pOpt.mDisplayHelp    = this.mDisplayHelp;
        pOpt.mLoggingLevel   = this.mLoggingLevel;
        pOpt.mForce          = this.mForce;
        pOpt.mCleanup        = this.mCleanup;
        pOpt.mSubmit         = this.mSubmit;
        pOpt.mGenRandomDir   = this.mGenRandomDir;
        pOpt.mOptArg         = this.mOptArg;
        pOpt.mMonitor        = this.mMonitor;
        pOpt.mRandomDirName  = this.mRandomDirName;
        pOpt.mAuthenticate   = this.mAuthenticate;
        pOpt.mClusterer      = this.mClusterer;
        pOpt.mBasenamePrefix = this.mBasenamePrefix;
        pOpt.mVOGroup        = this.mVOGroup;
        pOpt.mDeferredRun    = this.mDeferredRun;
        pOpt.mDate           = (Date)this.mDate.clone();
        //Note not cloning the vdsProps
        pOpt.mVDSProps       = null;
        return pOpt;
    }


    /**
     * Generates a Set by parsing a comma separated string.
     *
     * @param str   the comma separted String.
     *
     * @return Set containing the parsed values, in case of a null string
     *             an empty set is returned.
     */
    private Set generateSet(String str){
        Set s = new HashSet();

        //check for null
        if( s == null ) { return s; }

        for ( StringTokenizer st = new StringTokenizer(str,","); st.hasMoreElements(); ){
            s.add(st.nextToken().trim());
        }

        return s;
    }


    /**
     * A small utility method that santizes the url, converting it from
     * relative to absolute. In case the path is relative, it uses the
     * System property user.dir to get the current working directory, from
     * where the planner is being run.
     *
     * @param path the absolute or the relative path.
     *
     * @return the absolute path.
     */
    private String sanitizePath( String path ){
        String absPath;
        char separator = File.separatorChar;

        absPath = (path.indexOf(separator) == 0)?
                  //absolute path given already
                  path:
                  //get the current working dir
                  System.getProperty( "user.dir" ) + separator
                  + ( ( path.indexOf( '.' ) == 0 )? //path starts with a . ?
                      ( (path.indexOf( separator ) == 1 ) ? //path starts with a ./ ?
                                                         path.substring( 2 ):
                                                         ( path.length() > 1 && path.charAt( 1 )  == '.' )? //path starts with .. ?
                                                                                       path: //keep path as it is
                                                                                       path.substring( path.indexOf( '.' ) + 1 )
                      )
                      : path );

        //remove trailing separator if any
        absPath = (absPath.lastIndexOf(separator) == absPath.length() - 1)?
                   absPath.substring(0, absPath.length() - 1):
                   absPath;

        return absPath;
    }


}
