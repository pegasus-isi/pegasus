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
package edu.isi.pegasus.planner.selector.site;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import java.util.List;
import java.util.Iterator;
import org.griphyn.cPlanner.namespace.Hints;

/**
 * The base class for the site selectors that want to map one job at a time.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractPerJob extends Abstract {

    /**
     * Maps the jobs in the workflow to the various grid sites.
     *
     * @param workflow the workflow in a Graph form.
     * @param sites the list of <code>String</code> objects representing the
     *              execution sites that can be used.
     *
     */
    public void mapWorkflow(Graph workflow, List sites) {
        //iterate through the jobs in BFS
        for (Iterator it = workflow.iterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            
            SubInfo job = (SubInfo) node.getContent();
            
            //only map a job for which execute site hint
            //is not specified in the DAX
            if( !job.hints.containsKey( Hints.EXECUTION_POOL_KEY ) ){
                mapJob( job, sites);
            }
        }

    }

    /**
     * Maps a job in the workflow to the various grid sites.
     *
     * @param job    the job to be mapped.
     * @param sites  the list of <code>String</code> objects representing the
     *               execution sites that can be used.
     *
     */
    public abstract void mapJob( SubInfo job, List sites ) ;

}
