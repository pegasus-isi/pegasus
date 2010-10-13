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


package org.griphyn.cPlanner.partitioner.graph;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PCRelation;



import java.util.Iterator;

/**
 * A Adapter class that converts the <code>ADag</code> to <code>Graph</code> and
 * vice a versa.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Adapter {


    /**
     * Converts the <code>ADag</code> to <code>Graph</code> instance.
     *
     * @param dag  the <code>ADag</code> object.
     *
     * @return  it's representation as a <code>Graph</code> instance.
     */
    public static Graph convert( ADag dag ){
        Graph graph = new MapGraph();

        //iterate through the list of jobs and populate the nodes in the graph
        for( Iterator it = dag.vJobSubInfos.iterator(); it.hasNext(); ){
            //pass the jobs to the callback
            //populate the job as a node in the graph
            SubInfo job = ( SubInfo )it.next();
            GraphNode node = new GraphNode( job.getID(), job );
            graph.addNode( node );
        }

        //add the edges between the nodes in the graph
        for( Iterator it = dag.dagInfo.relations.iterator(); it.hasNext(); ){
            PCRelation rel = (PCRelation) it.next();
            graph.addEdge( rel.getParent(), rel.getChild() );
        }

        return graph;

    }

}
