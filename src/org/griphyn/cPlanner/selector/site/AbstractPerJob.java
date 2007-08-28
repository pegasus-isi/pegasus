package org.griphyn.cPlanner.selector.site;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import java.util.List;
import java.util.Iterator;

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
            mapJob( (SubInfo) node.getContent(), sites);
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
