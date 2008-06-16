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

package org.griphyn.cPlanner.engine;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;

import java.util.Iterator;
import java.util.Set;


/**
 * This ends up placing the create directory jobs at the top of the graph.
 * However instead of constricting it to an hour glass shape, this class links
 * it to all the relevant nodes for which the create dir job is necessary. It is
 * like that it spreads its tentacles all around. This potentially ends up
 * putting more load on the DagMan with all the dependencies but removes the
 * restriction of the plan progressing only when all the create directory
 * jobs have progressed on the remote pools, as in the HourGlass model.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public class Tentacles extends CreateDirectory {


    /**
     * Default constructor.
     *
     * @param concDag  The concrete dag so far.
     * @param bag      bag of initialization objects
     */
    public Tentacles( ADag concDag, PegasusBag bag ) {
        super( concDag, bag );
    }

    /**
     * Adds the create directory nodes into the workflow, with each create
     * dir job as a parent to each job that has been scheduled on that
     * particular pool.
     */
    public void addCreateDirectoryNodes() {
        Set set        = this.getCreateDirSites();
        String pool    = null;
        String jobName = null;
        String parent  = null;

	//traverse through the jobs and
        //looking at their execution pool
        //and create a dependency to the
        //the correct create node
	//we add links first and jobs later

        //remove the entry for the local pool
        //set.remove("local");
        SubInfo job;
        int type;
        boolean local;
        for(Iterator it = mCurrentDag.vJobSubInfos.iterator();it.hasNext();){
            job  = (SubInfo)it.next();
            jobName = job.getName();
            pool    = job.getSiteHandle();

            //the parent in case of a transfer job
            //is the non third party site
            parent = (job instanceof TransferJob)?
                    getCreateDirJobName(((TransferJob)job).getNonThirdPartySite()):
                    getCreateDirJobName(pool);

            //put in the dependency only for transfer jobs that stage in data
            //or are jobs running on remote sites
            //or are compute jobs running on local site
            type = job.getJobType();
            local = pool.equals("local");
            if( (job instanceof TransferJob && type == SubInfo.STAGE_IN_JOB)
                || (!local
                          || (type == SubInfo.COMPUTE_JOB || type == SubInfo.STAGED_COMPUTE_JOB))){
                mLogger.log("Adding relation " + parent + " -> " + jobName,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                mCurrentDag.addNewRelation(parent,jobName);
            }
        }


        //for each execution pool add
        //a create directory node.
        SubInfo newJob = null;
        for (Iterator it = set.iterator();it.hasNext();){
            pool    = (String)it.next();
            jobName = getCreateDirJobName(pool);
            newJob  = makeCreateDirJob(pool,jobName);
            mCurrentDag.add(newJob);

        }

    }

}
