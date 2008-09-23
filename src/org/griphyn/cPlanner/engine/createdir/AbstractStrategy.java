/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.engine.createdir;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.partitioner.graph.Graph;

import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author  Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractStrategy implements Strategy {
    
    /**
     * Constant suffix for the names of the create directory nodes.
     */
    public static final String CREATE_DIR_SUFFIX = "_cdir";


    /**
     * The handle to the logging object, that is used to log the messages.
     */
    protected LogManager mLogger;

    /**
     * The job prefix that needs to be applied to the job file basenames.
     */
    protected String mJobPrefix;

    /**
     * Whether we want to use dirmanager or mkdir directly.
     */
    protected boolean mUseMkdir;
    
    /**
     * The implementation instance that is used to create a create dir job.
     */
    protected Implementation mImpl;
    
    /**
     * The Site Store handle.
     */
    protected SiteStore mSiteStore;
    
    /**
     * Intializes the class.
     *
     * @param bag    bag of initialization objects
     * @param imp    the implementation instance that creates create dir job 
     */
    public void initialize( PegasusBag bag, Implementation impl ){
        mImpl       = impl;
        mJobPrefix  = bag.getPlannerOptions().getJobnamePrefix();
        mLogger     = bag.getLogger();
        mSiteStore  = bag.getHandleToSiteStore();
        
        //in case of staging of executables/worker package
        //we use mkdir directly
        mUseMkdir = bag.getHandleToTransformationMapper().isStageableMapper();
    }

    
    /**
     * It returns the name of the create directory job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param dag   the workflow to which the create dir jobs are being added.
     * @param pool  the execution pool for which the create directory job
     *                  is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    public String getCreateDirJobName( ADag dag, String pool){
        StringBuffer sb = new StringBuffer();


        sb.append( dag.dagInfo.nameOfADag).append("_").
           append( dag.dagInfo.index).append("_");

       //append the job prefix if specified in options at runtime
       if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

       sb.append(pool).append(this.CREATE_DIR_SUFFIX);

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
    protected Set getCreateDirSites( ADag dag ){
        Set set = new HashSet();

        for( Iterator it = dag.vJobSubInfos.iterator();it.hasNext();){
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


}
