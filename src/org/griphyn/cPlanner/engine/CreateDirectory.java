/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.UserOptions;

import org.griphyn.common.catalog.TransformationCatalogEntry;
import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

/**
 * This common interface that identifies the basic functions that need to be
 * implemented to introduce random directories in which the jobs are executed on
 * the remote execution pools. The implementing classes are invoked when the user
 * gives the --randomdir option. The implementing classes determine where in the
 * graph the nodes creating the random directories are placed and their
 * dependencies with the rest of the nodes in the graph.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public abstract class CreateDirectory
    extends Engine {

    /**
     * Constant suffix for the names of the create directory nodes.
     */
    public static final String CREATE_DIR_SUFFIX = "_cdir";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String CREATE_DIR_TRANSFORMATION = "dirmanager";

    /**
     * The name of the package in which all the implementing classes are.
     */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.engine.";

    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NS = null;

    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NS = "Pegasus";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * It is a reference to the Concrete Dag so far.
     */
    protected ADag mCurrentDag;

    /**
     * The handle to the options specified by the user at runtime. The name of
     * the random directory is picked up from here.
     */
    protected UserOptions mUserOpts;

    /**
     * The handle to the logging object, that is used to log the messages.
     */
    protected LogManager mLogger;


    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime.
     *
     * @param className  The name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package.
     *                   That is added by itself.
     * @param dag        the workflow.
     * @param properties the <code>PegasusProperties</code> to be used.
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static CreateDirectory loadCreateDirectoryInstance(
                                                              String className,
                                                              ADag concDag,
                                                              PegasusProperties properties ) throws FactoryException {

        //prepend the package name
        className = PACKAGE_NAME + className;

        //try loading the class dynamically
        CreateDirectory cd = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[2];
            argList[0] = concDag;
            argList[1] = properties;
            cd = (CreateDirectory) dl.instantiate(argList);
        } catch (Exception e) {
            throw new FactoryException( "Instantiating Create Directory",
                                        className,
                                        e );
        }

        return cd;
    }

    /**
     * A pratically nothing constructor !
     *
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    protected CreateDirectory( PegasusProperties properties ) {
        super( properties );
        mCurrentDag = null;
        mUserOpts   = UserOptions.getInstance();
        mTCHandle   = TCMode.loadInstance();
        mLogger     = LogManager.getInstance();
    }

    /**
     * Default constructor.
     *
     * @param concDag  The concrete dag so far.
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    protected CreateDirectory( ADag concDag, PegasusProperties properties ) {
        super( properties );
        mCurrentDag = concDag;
        mUserOpts = UserOptions.getInstance();
        mTCHandle = TCMode.loadInstance();
        mLogger   = LogManager.getInstance();
    }

    /**
     * It modifies the concrete dag passed in the constructor and adds the create
     * random directory nodes to it at the root level. These directory nodes have
     * a common child that acts as a concatenating job and ensures that Condor
     * does not start staging in the data before the directories have been added.
     * The root nodes in the unmodified dag are now chidren of this concatenating
     * dummy job.
     */
    public abstract void addCreateDirectoryNodes();

    /**
     * It returns the name of the create directory job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param pool  the execution pool for which the create directory job
     *                  is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    protected String getCreateDirJobName(String pool){
        StringBuffer sb = new StringBuffer();
        sb.append(mCurrentDag.dagInfo.nameOfADag).append("_").
           append(mCurrentDag.dagInfo.index).append("_").
           append(pool).append(this.CREATE_DIR_SUFFIX);

       return sb.toString();
    }

    /**
     * Retrieves the sites for which the create dir jobs need to be created.
     * It returns all the sites where the compute jobs have been scheduled.
     *
     *
     * @return  a Set containing a list of siteID's of the sites where the
     *          dag has to be run.
     */
    protected Set getCreateDirSites(){
        Set set = new HashSet();

        for(Iterator it = mCurrentDag.vJobSubInfos.iterator();it.hasNext();){
            SubInfo job = (SubInfo)it.next();
            //add to the set only if the job is
            //being run in the work directory
            //this takes care of local site create dir
            if(job.runInWorkDirectory()){
                set.add(job.executionPool);
            }
        }

        //remove the stork pool
        set.remove("stork");

        return set;
    }



    /**
     * It creates a make directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * It gets the name of the random directory from the Pool handle. This method
     * does not update the internal graph structure of the workflow to add the
     * node. That is done separately.
     *
     * @param execPool  the execution pool for which the create dir job is to be
     *                  created.
     * @param jobName   the name that is to be assigned to the job.
     */
    protected SubInfo makeCreateDirJob(String execPool, String jobName) {
        SubInfo newJob  = new SubInfo();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
        JobManager jobManager = null;

        try {
            entries = mTCHandle.getTCEntries(null,
                                               this.CREATE_DIR_TRANSFORMATION, null,
                                               execPool, TCType.INSTALLED);

            if(entries == null){
                //log a message and exit
                StringBuffer error = new StringBuffer();
                error.append( "Unable to map transformation " ).
                      append( this.CREATE_DIR_TRANSFORMATION ).append( " on site " ).
                      append( execPool );
                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( error.toString() );
            }
            else{
                entry = (TransformationCatalogEntry) entries.get(0);
                execPath = entry.getPhysicalTransformation();
            }

        } catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.ERROR_MESSAGE_LEVEL );
        }

        SiteInfo ePool = mPoolHandle.getPoolEntry(execPool, "transfer");
        jobManager = ePool.selectJobManager("transfer",true);
        String argString = "--create --dir " +
            mPoolHandle.getExecPoolWorkDir(execPool);

        newJob.jobName = jobName;
        newJob.logicalName = this.CREATE_DIR_TRANSFORMATION;
        newJob.namespace = this.TRANSFORMATION_NS;
        newJob.version = null;
        newJob.dvName = this.CREATE_DIR_TRANSFORMATION;
        newJob.dvNamespace = this.DERIVATION_NS;
        newJob.dvVersion = this.DERIVATION_VERSION;
        newJob.condorUniverse = "vanilla";
        newJob.globusScheduler = jobManager.getInfo(JobManager.URL);
        newJob.executable = execPath;
        newJob.executionPool = execPool;
        newJob.strargs = argString;
        newJob.jobClass = SubInfo.CREATE_DIR_JOB;
        newJob.jobID = jobName;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles(mPoolHandle.getPoolProfile(newJob.executionPool));

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        newJob.updateProfiles(entry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        newJob.updateProfiles(mProps);

        return newJob;

    }


}
