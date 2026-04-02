/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.LinkedList;
import java.util.List;

/**
 * A stub data flow job for DECAF integration
 *
 * @author Karan Vahi
 */
public class DataFlowJob extends AggregatedJob {

    private List<Link> mEdges;

    /**
     * A boolean to track whether Data Flow Job is partially created or not. In case of job
     * clustering using DECAF, we need to distinguish the state of the job. By default, a
     * DataFlowJob if specified in the Abstract Workflow is always fully created.
     */
    private boolean mPartiallyCreated;

    /** The default constructor. */
    public DataFlowJob() {
        super();
        mEdges = new LinkedList();
        // data flow job cannot be executed via kickstart
        this.vdsNS.construct(Pegasus.GRIDSTART_KEY, "none");
        mPartiallyCreated = false;
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     */
    public DataFlowJob(Job job) {
        this(job, -1);
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     * @param num the number of constituent jobs.
     */
    public DataFlowJob(Job job, int num) {
        super(job, num);
        mEdges = new LinkedList();
        // data flow job cannot be executed via kickstart
        this.vdsNS.construct(Pegasus.GRIDSTART_KEY, "none");
        mPartiallyCreated = false;
    }

    /**
     * Add Link
     *
     * @param e
     */
    public void addEdge(Link e) {
        mEdges.add(e);
    }

    /**
     * Returns a boolean indicating if the job is partially created or not
     *
     * @return boolean
     */
    public boolean isPartiallyCreated() {
        return this.mPartiallyCreated;
    }

    /** Sets the internal boolean flag that indicates job is only partially created. */
    public void setPartiallyCreated() {
        this.setPartiallyCreated(true);
    }

    /**
     * Set the partially created flag to value passed
     *
     * @param value boolean
     */
    public void setPartiallyCreated(boolean value) {
        this.mPartiallyCreated = value;
    }

    /**
     * A link job to indicate a job in the data flow that does data transformation between two jobs
     */
    public static class Link extends Job {

        private GraphNode mParentJob;
        private GraphNode mChildJob;

        public Link() {
            super();
        }

        public void setLink(String parent, String child) {
            mParentJob = new GraphNode(parent);
            mChildJob = new GraphNode(child);
        }

        public String getParentID() {
            return mParentJob.getID();
        }

        public String getChildID() {
            return mChildJob.getID();
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException(
                    "Not supported yet."); // To change body of generated methods, choose Tools |
            // Templates.
        }
    }
}
