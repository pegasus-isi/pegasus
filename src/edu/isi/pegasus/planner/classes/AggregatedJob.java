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

import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.MapGraph;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class holds all the specifics of an aggregated job. An aggregated job or a clustered job is
 * a job, that contains a collection of smaller jobs. An aggregated job during execution may explode
 * into n smaller job executions. At present it does not store information about the dependencies
 * between the jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class AggregatedJob extends Job implements Graph {

    /** The collection of jobs that are contained in the aggregated job. */
    //    private List mConstituentJobs;

    /**
     * Boolean indicating whether a job has been fully rendered to an executable job or not i.e the
     * aggregated job has been mapped to the aggregator and the constituent jobs have been
     * gridstarted or not.
     */
    private boolean mHasBeenRenderedToExecutableForm;

    /** Handle to the JobAggregator that created this job. */
    private JobAggregator mJobAggregator;

    /** Handle to the Graph implementor. */
    private Graph mGraphImplementor;

    /** The default constructor. */
    public AggregatedJob() {
        super();
        //        mConstituentJobs = new ArrayList(3);
        mHasBeenRenderedToExecutableForm = false;
        this.mJobAggregator = null;
        mGraphImplementor = new MapGraph();
    }

    /**
     * The overloaded constructor.
     *
     * @param num the number of constituent jobs
     */
    public AggregatedJob(int num) {
        this();
        //        mConstituentJobs = new ArrayList(num);
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     * @param num the number of constituent jobs.
     */
    public AggregatedJob(Job job, int num) {
        super((Job) job.clone());
        //        mConstituentJobs = new ArrayList(num);
        mHasBeenRenderedToExecutableForm = false;
        this.mJobAggregator = null;
        this.mGraphImplementor = new MapGraph();
    }

    /**
     * Returns a boolean indicating whether a job has been rendered to an executable form or not
     *
     * @return boolean
     */
    public boolean renderedToExecutableForm() {
        return this.mHasBeenRenderedToExecutableForm;
    }

    /**
     * Returns a boolean indicating whether a job has been rendered to an executable form or not
     *
     * @param value boolean to set to.
     */
    public void setRenderedToExecutableForm(boolean value) {
        this.mHasBeenRenderedToExecutableForm = value;
    }

    /**
     * Sets the JobAggregator that created this aggregated job. Useful for rendering the job to an
     * executable form later on.
     *
     * @param aggregator handle to the JobAggregator used for aggregating the job
     */
    public void setJobAggregator(JobAggregator aggregator) {
        this.mJobAggregator = aggregator;
    }

    /**
     * Returns the JobAggregator that created this aggregated job. Useful for rendering the job to
     * an executable form later on.
     *
     * @return JobAggregator
     */
    public JobAggregator getJobAggregator() {
        return this.mJobAggregator;
    }

    /**
     * Adds a job to the aggregated job.
     *
     * @param job the job to be added.
     */
    public void add(Job job) {
        //        mConstituentJobs.add(job);

        // should be getID() instead of logicalID
        // however that requires change in vertical clusterer where
        // partition object is indexed by logical id's
        this.addNode(new GraphNode(job.getLogicalID(), job));
    }

    /**
     * Clustered jobs never originate in the DAX. Always return null.
     *
     * @return null
     */
    public String getDAXID() {
        return null;
    }

    /**
     * Returns a new copy of the Object. The constituent jobs are also cloned.
     *
     * @return Object
     */
    public Object clone() {
        AggregatedJob newJob = new AggregatedJob((Job) super.clone(), this.size());
        /*
        for(Iterator it = this.mConstituentJobs.iterator();it.hasNext();){
            newJob.add( (Job)(((Job)it.next()).clone()));
        }*/
        // shallow clone. Fix me.
        newJob.mGraphImplementor = (Graph) ((MapGraph) this.mGraphImplementor).clone();

        newJob.mHasBeenRenderedToExecutableForm = this.mHasBeenRenderedToExecutableForm;
        newJob.mJobAggregator = this.mJobAggregator;

        return newJob;
    }

    /**
     * Returns an iterator to the constituent jobs of the AggregatedJob.
     *
     * @return Iterator
     */
    public Iterator<Job> constituentJobsIterator() {
        //        return mConstituentJobs.iterator();
        // need to use the toplogical sort iterator for label based clustering
        List<Job> l = new LinkedList();
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ) {
            GraphNode n = it.next();
            l.add((Job) n.getContent());
        }
        return l.iterator();
    }

    /**
     * Returns a job from a particular position in the list of constituent jobs
     *
     * @param index the index to retrieve from
     * @return a constituent job.
     */
    public Job getConstituentJob(int index) {
        //        return (Job) this.mConstituentJobs.get( index );

        // should be deprecated
        int i = 0;
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); i++) {
            GraphNode n = it.next();
            if (i == index) {
                return (Job) n.getContent();
            }
        }
        return null;
    }

    /**
     * Returns the number of constituent jobs.
     *
     * @return Iterator
     */
    public int numberOfConsitutentJobs() {
        return this.size();
    }

    /**
     * Sets the relative submit directory for the job. The directory is relative to the top level
     * directory where the workflow files are placed. It traverses through the internal node list,
     * to ensure constituent jobs that are clustered jobs themselves are assigned the relative
     * submit directory correctly.
     *
     * @param dir the directory
     */
    @Override
    public void setRelativeSubmitDirectory(String dir) {
        super.setRelativeSubmitDirectory(dir);
        // PM-833 traverse through the internal list
        for (Iterator it = this.nodeIterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            Job constituentJob = (Job) node.getContent();
            if (constituentJob instanceof AggregatedJob) {
                // PM-833 we need to make sure clustered job part of larger
                // cluster also has it set
                ((AggregatedJob) constituentJob).setRelativeSubmitDirectory(dir);
            }
        }
    }

    /**
     * Returns a textual description of the object.
     *
     * @return textual description of the job.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(32);
        sb.append("\n").append("[MAIN JOB]").append(super.toString());
        sb.append("\n").append("[CONSTITUENT JOBS]");
        int num = 0;
        for (Iterator it = this.nodeIterator(); it.hasNext(); ++num) {
            sb.append("\n").append("[CONSTITUENT JOB] :").append(num);
            sb.append(it.next());
        }
        return sb.toString();
    }

    /**
     * Adds a node to the Graph. It overwrites an already existing node with the same ID.
     *
     * @param node the node to be added to the Graph.
     */
    public void addNode(GraphNode node) {
        this.mGraphImplementor.addNode(node);
    }

    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent the parent node ID.
     * @param child the child node ID.
     */
    public void addEdge(String parent, String child) {
        this.mGraphImplementor.addEdge(parent, child);
    }

    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent the parent node .
     * @param child the child node .
     */
    public void addEdge(GraphNode parent, GraphNode child) {
        this.mGraphImplementor.addEdge(parent, child);
    }

    /**
     * A convenience method that allows for bulk addition of edges between already existing nodes in
     * the graph.
     *
     * @param child the child node ID
     * @param parents list of parent identifiers as <code>String</code>.
     */
    public void addEdges(String child, List parents) {
        this.mGraphImplementor.addEdges(child, parents);
    }

    /**
     * Returns the node matching the id passed.
     *
     * @param identifier the id of the node.
     * @return the node matching the ID else null.
     */
    public GraphNode getNode(String identifier) {
        return this.mGraphImplementor.getNode(identifier);
    }

    /**
     * Adds a single root node to the Graph. All the exisitng roots of the Graph become children of
     * the root.
     *
     * @param root the <code>GraphNode</code> to be added as a root.
     * @throws RuntimeException if a node with the same id already exists.
     */
    public void addRoot(GraphNode root) {
        this.mGraphImplementor.addRoot(root);
    }

    /**
     * Removes a node from the Graph.
     *
     * @param identifier the id of the node to be removed.
     * @return boolean indicating whether the node was removed or not.
     */
    public boolean remove(String identifier) {
        return this.mGraphImplementor.remove(identifier);
    }

    /**
     * Resets all the dependencies in the Graph, while preserving the nodes. The resulting Graph is
     * a graph of independent nodes.
     */
    public void resetEdges() {
        this.mGraphImplementor.resetEdges();
    }

    /**
     * Returns an iterator for the nodes in the Graph. These iterators are fail safe.
     *
     * @return Iterator
     */
    public Iterator<GraphNode> nodeIterator() {
        return this.mGraphImplementor.nodeIterator();
    }

    /**
     * Returns an iterator that traverses through the graph using a graph traversal algorithm.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> iterator() {
        return this.mGraphImplementor.iterator();
    }

    /**
     * Returns an iterator that traverses the graph bottom up from the leaves. At any one time, only
     * one iterator can iterate through the graph.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator bottomUpIterator() {
        return this.mGraphImplementor.bottomUpIterator();
    }

    /**
     * Returns an iterator for the graph that traverses in topological sort order.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> topologicalSortIterator() {
        return this.mGraphImplementor.topologicalSortIterator();
    }

    /** Returns the number of nodes in the graph. */
    public int size() {
        return this.mGraphImplementor.size();
    }

    /**
     * Returns the root nodes of the Graph.
     *
     * @return a list containing <code>GraphNode</code> corressponding to the root nodes.
     */
    public List<GraphNode> getRoots() {
        return this.mGraphImplementor.getRoots();
    }

    /**
     * Returns the leaf nodes of the Graph.
     *
     * @return a list containing <code>GraphNode</code> corressponding to the leaf nodes.
     */
    public List<GraphNode> getLeaves() {
        return this.mGraphImplementor.getLeaves();
    }

    /**
     * Returns a boolean if there are no nodes in the graph.
     *
     * @return boolean
     */
    public boolean isEmpty() {
        return this.mGraphImplementor.isEmpty();
    }

    /**
     * Returns a boolean indicating whether a graph has cyclic edges or not.
     *
     * @return boolean
     */
    public boolean hasCycles() {
        return this.mGraphImplementor.hasCycles();
    }

    /**
     * Returns the detected cyclic edge if , hasCycles returns true
     *
     * @return
     */
    public NameValue getCyclicEdge() {
        return this.mGraphImplementor.getCyclicEdge();
    }
}
