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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.MapGraph;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This class object contains the info about a Dag. DagInfo object contains the information to
 * create the .dax file. vJobSubInfos is a Vector containing Job objects of jobs making the Dag.
 * Each subinfo object contains information needed to generate a submit file for that job.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 * @see DagInfo
 * @see Job
 */
public class ADag extends Data implements Graph {

    /** The DagInfo object which contains the information got from parsing the dax file. */
    private DagInfo mDAGInfo;

    /**
     * The root of the submit directory hierarchy for the DAG. This is the directory where generally
     * the DAG related files like the log files, .dag and dagman output files reside.
     */
    private String mSubmitDirectory;

    /** The optional request ID associated with the DAX. */
    private String mRequestID;

    /** Handle to the replica store that stores the replica catalog user specifies in the DAX */
    protected ReplicaStore mReplicaStore;

    /**
     * Handle to the transformation store that stores the transformation catalog user specifies in
     * the DAX
     */
    protected TransformationStore mTransformationStore;

    /** Handle to the Site store that stores the site catalog entries specified in the DAX */
    protected SiteStore mSiteStore;

    /** The Root Workflow UUID. */
    @Expose
    @SerializedName("root_workflow_uuid")
    protected String mRootWorkflowUUID;

    /** The UUID associated with the workflow. */
    @Expose
    @SerializedName("workflow_uuid")
    protected String mWorkflowUUID;

    /** Boolean indicating whether the refinement process on the workflow has started or not. */
    protected boolean mWorkflowRefinementStarted;

    /** All the notifications associated with the job */
    protected Notifications mNotifications;

    /** The profiles associated with the site. */
    @Expose
    @SerializedName("wf_metadata")
    private Profiles mProfiles;

    /** Handle to the Graph implementor. */
    @Expose
    @SerializedName("graph")
    private Graph mGraphImplementor;

    /** Initialises the class member variables. */
    public ADag() {
        mDAGInfo = new DagInfo();
        mSubmitDirectory = ".";
        mWorkflowUUID = generateWorkflowUUID();
        mRootWorkflowUUID = null;
        mWorkflowRefinementStarted = false;
        mNotifications = new Notifications();
        mGraphImplementor = new MapGraph();
        mProfiles = new Profiles();
        resetStores();
    }

    /**
     * Adds a Invoke object corresponding to a notification.
     *
     * @param invoke the invoke object containing the notification
     */
    public void addNotification(Invoke invoke) {
        this.mNotifications.add(invoke);
    }

    /**
     * Adds all the notifications passed to the underlying container.
     *
     * @param invokes the notifications to be added
     */
    public void addNotifications(Notifications invokes) {
        this.mNotifications.addAll(invokes);
    }

    /**
     * Add metadata to the object.
     *
     * @param key
     * @param value
     */
    public void addMetadata(String key, String value) {
        this.mProfiles.addProfile(Profiles.NAMESPACES.metadata, key, value);
    }

    /**
     * Returns metadata attribute for a particular key
     *
     * @param key
     * @return value returned else null if not found
     */
    public String getMetadata(String key) {
        return (String) mProfiles.get(Profiles.NAMESPACES.metadata).get(key);
    }

    /**
     * Returns all metadata attributes for the file
     *
     * @return Metadata
     */
    public Metadata getAllMetadata() {
        return (Metadata) mProfiles.get(Profiles.NAMESPACES.metadata);
    }

    /**
     * Returns a collection of all the notifications that need to be done for a particular condition
     *
     * @param when the condition
     * @return
     */
    public Collection<Invoke> getNotifications(Invoke.WHEN when) {
        return this.mNotifications.getNotifications(when);
    }

    /**
     * Returns all the notifications associated with the job.
     *
     * @return the notifications
     */
    public Notifications getNotifications() {
        return this.mNotifications;
    }

    /** Resets the replica and transformation stores; */
    public void resetStores() {
        this.mReplicaStore = new ReplicaStore();
        this.mTransformationStore = new TransformationStore();
        this.mSiteStore = new SiteStore();
    }

    /**
     * Returns a new copy of the Object.
     *
     * @return the clone of the object.
     */
    public Object clone() {
        ADag newAdag = new ADag();

        newAdag.setBaseSubmitDirectory(this.mSubmitDirectory);
        newAdag.setRequestID(this.mRequestID);
        newAdag.setRootWorkflowUUID(this.getRootWorkflowUUID());
        newAdag.setWorkflowRefinementStarted(this.mWorkflowRefinementStarted);
        // the stores are not a true clone
        newAdag.setReplicaStore(mReplicaStore);
        newAdag.setTransformationStore(mTransformationStore);
        newAdag.setSiteStore(mSiteStore);

        newAdag.setProfiles((Profiles) this.mProfiles.clone());

        newAdag.setWorkflowUUID(this.getWorkflowUUID());
        newAdag.addNotifications(this.getNotifications());

        return newAdag;
    }

    /**
     * Returns the UUID for the Root workflow
     *
     * @return the UUID of the workflow
     */
    public String getRootWorkflowUUID() {
        return this.mRootWorkflowUUID;
    }

    /**
     * Sets the root UUID for the workflow
     *
     * @param uuid the UUID of the workflow
     */
    public void setRootWorkflowUUID(String uuid) {
        this.mRootWorkflowUUID = uuid;
    }

    /**
     * Returns the UUID for the workflow
     *
     * @return the UUID of the workflow
     */
    public String getWorkflowUUID() {
        return this.mWorkflowUUID;
    }

    /**
     * Sets the UUID for the workflow
     *
     * @param uuid the UUID of the workflow
     */
    public void setWorkflowUUID(String uuid) {
        this.mWorkflowUUID = uuid;
    }

    /**
     * Generates the UUID for the workflow
     *
     * @return the UUID of the workflow
     */
    protected String generateWorkflowUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * Returns a boolean indicating whether the workflow refinement has started or not
     *
     * @return boolean
     */
    public boolean hasWorkflowRefinementStarted() {
        return this.mWorkflowRefinementStarted;
    }

    /**
     * Sets whether the workflow refinement has started or not
     *
     * @param state the boolean value
     */
    public void setWorkflowRefinementStarted(boolean state) {
        this.mWorkflowRefinementStarted = state;
    }

    /**
     * Returns the String description of the dag associated with this object.
     *
     * @return textual description.
     */
    public String toString() {
        String newLine = System.getProperty("line.separator", "\r\n");
        StringBuilder sb = new StringBuilder();
        sb.append("Submit Directory ")
                .append(this.mSubmitDirectory)
                .append(newLine)
                .append("Root Workflow UUID ")
                .append(this.getRootWorkflowUUID())
                .append(newLine)
                .append("Workflow UUID ")
                .append(this.getWorkflowUUID())
                .append(newLine)
                .append("Workflow Refinement Started ")
                .append(this.hasWorkflowRefinementStarted())
                .append(newLine);

        sb.append("DAG Structure ").append(newLine);
        // lets write out the edges
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();
            sb.append("JOB").append(" ").append(gn.getID()).append(newLine);
        }

        // lets write out the edges
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();

            // get a list of parents of the node
            for (GraphNode child : gn.getChildren()) {
                sb.append("EDGE")
                        .append(" ")
                        .append(gn.getID())
                        .append(" -> ")
                        .append(child.getID())
                        .append(newLine);
            }
        }

        return sb.toString();
    }

    /**
     * This adds a new job to the ADAG object. It ends up adding both the job name and the job
     * description to the internal structure.
     *
     * @param job the new job that is to be added to the ADag.
     */
    public void add(Job job) {
        this.addNode(new GraphNode(job.getID(), job));
    }

    /**
     * Removes a particular job from the workflow. It however does not delete the relations the
     * edges that refer to the job.
     *
     * @param job the <code>Job</code> object containing the job description.
     * @return boolean indicating whether the removal was successful or not.
     */
    public boolean remove(Job job) {
        return this.remove(job.getID());
    }

    /**
     * Returns the number of jobs in the dag on the basis of number of elements in the <code>dagJobs
     * </code> Vector.
     *
     * @return the number of jobs.
     */
    public int getNoOfJobs() {
        return this.size();
        //       return this.mDAGInfo.getNoOfJobs();
    }

    /**
     * Sets the request id.
     *
     * @param id the request id.
     */
    public void setRequestID(String id) {
        mRequestID = id;
    }

    /**
     * Returns the request id.
     *
     * @return the request id.
     */
    public String getRequestID() {
        return mRequestID;
    }

    /**
     * Returns the workflow id
     *
     * @return the abstract workflow id
     */
    public String getAbstractWorkflowName() {
        StringBuffer id = new StringBuffer();
        id.append(this.mDAGInfo.getLabel()).append("-").append(this.mDAGInfo.getIndex());
        return id.toString();
    }

    /**
     * Returns the workflow id
     *
     * @return the executable workflow id
     */
    public String getExecutableWorkflowName() {
        StringBuffer id = new StringBuffer();
        id.append(this.mDAGInfo.getLabel())
                .append("_")
                .append(this.mDAGInfo.getIndex())
                .append(".")
                .append("dag");
        return id.toString();
    }

    /**
     * Adds a new PCRelation pair to the Vector of <code>PCRelation</code> pairs. For the new
     * relation the isDeleted parameter is set to false.
     *
     * @param parent The parent in the relation pair
     * @param child The child in the relation pair
     */
    public void addNewRelation(String parent, String child) {
        this.addEdge(parent, child);
        /*        PCRelation newRelation = new PCRelation(parent,child);
               this.mDAGInfo.relations.addElement(newRelation);
        */
    }

    /**
     * Sets the submit directory for the workflow.
     *
     * @param dir the submit directory.
     */
    public void setBaseSubmitDirectory(String dir) {
        this.mSubmitDirectory = dir;
    }

    /**
     * Returns the label of the workflow, that was specified in the DAX.
     *
     * @return the label of the workflow.
     */
    public String getLabel() {
        return this.mDAGInfo.getLabel();
    }

    /**
     * Sets the label for the workflow.
     *
     * @param label the label to be assigned to the workflow
     */
    public void setLabel(String label) {
        this.mDAGInfo.setLabel(label);
    }

    /**
     * Returns the index of the workflow, that was specified in the DAX.
     *
     * @return the index of the workflow.
     */
    public String getIndex() {
        return this.mDAGInfo.getIndex();
    }

    /**
     * Set the index of the workflow, that was specified in the DAX.
     *
     * @param index the count
     */
    public void setIndex(String index) {
        this.mDAGInfo.setIndex(index);
    }

    /**
     * Set the count of the workflow, that was specified in the DAX.
     *
     * @param count the count
     */
    public void setCount(String count) {
        this.mDAGInfo.setCount(count);
    }

    /**
     * Returns the count of the workflow, that was specified in the DAX.
     *
     * @return the count
     */
    public String getCount() {
        return this.mDAGInfo.getCount();
    }

    /**
     * Returns the dax version
     *
     * @return teh dax version.
     */
    public String getDAXVersion() {
        return this.mDAGInfo.getDAXVersion();
    }

    /**
     * Returns the Abstract Workflow API associated as metadata
     *
     * @return the value
     */
    public String getWFAPI() {
        // PM-1654 starting 5.0 it is wf_api. if that is not present then 
        // fall back to dax_api that was the previous key
        String wfAPI = this.getMetadata(Metadata.WF_API_KEY);
        wfAPI = (wfAPI == null) ? this.getMetadata(Metadata.DAX_API_KEY) : wfAPI;
        wfAPI = (wfAPI == null) ? Metadata.DEFAULT_DAX_API : wfAPI;
        return wfAPI;
    }

    /**
     * Returns the last modified time for the file containing the workflow description.
     *
     * @return the MTime
     */
    public String getMTime() {
        return this.mDAGInfo.getMTime();
    }

    /**
     * Returns the root of submit directory hierarchy for the workflow.
     *
     * @return the directory.
     */
    public String getBaseSubmitDirectory() {
        return this.mSubmitDirectory;
    }

    /**
     * Checks the underlying graph structure for any corruption. Corruption can be where a parent or
     * a child of a node refers to an object, that is not in underlying graph node list.
     *
     * @throws RuntimeException in case of corruption.
     */
    public void checkForCorruption() {
        Set<GraphNode> s = Collections.newSetFromMap(new IdentityHashMap());

        // put all the nodes in the idendity backed set
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ) {
            s.add(it.next());
        }

        // now again traverse and make sure all the parents and children
        // of each node exist in the set
        for (Iterator<GraphNode> it = this.nodeIterator(); it.hasNext(); ) {
            GraphNode node = it.next();

            for (GraphNode parent : node.getParents()) {
                // contains operation is on basis of underlying IdentityHashMap
                if (!s.contains(parent)) {
                    throw new RuntimeException(complain("Parent", node, parent));
                }
            }

            for (GraphNode child : node.getChildren()) {
                if (!s.contains(child)) {
                    throw new RuntimeException(complain("Child", node, child));
                }
            }
        }
    }

    /**
     * Convenience method to complain for a linked node from a node that does not exist in the DAG
     *
     * @param desc
     * @param node
     * @param linkedNode
     */
    private String complain(String desc, GraphNode node, GraphNode linkedNode) {

        StringBuilder error = new StringBuilder();
        error.append(desc)
                .append(" ")
                .append(linkedNode.getID())
                .append(" for node ")
                .append(node.getID())
                .append(" is corrupted. ");
        GraphNode fromNodeMap = this.getNode(linkedNode.getID());
        error.append("Two instances of node ")
                .append(linkedNode.getID())
                .append(" with identities ")
                .append(System.identityHashCode(fromNodeMap))
                .append(" and ")
                .append(System.identityHashCode(linkedNode));
        return error.toString();
    }

    /**
     * Sets the Replica Store
     *
     * @param store the Replica Store
     */
    public void setReplicaStore(ReplicaStore store) {
        this.mReplicaStore = store;
    }

    /**
     * Returns the Replica Store
     *
     * @return the Replica Store
     */
    public ReplicaStore getReplicaStore() {
        return this.mReplicaStore;
    }

    /**
     * Sets the Transformation Store
     *
     * @param store the Transformation Store
     */
    public void setTransformationStore(TransformationStore store) {
        this.mTransformationStore = store;
    }

    /**
     * Returns the Transformation Store
     *
     * @return the Transformation Store
     */
    public TransformationStore getTransformationStore() {
        return this.mTransformationStore;
    }

    /**
     * Sets the Site Store
     *
     * @param store the Site Store
     */
    public void setSiteStore(SiteStore store) {
        this.mSiteStore = store;
    }

    /**
     * Returns the Site Store
     *
     * @return the Site Store
     */
    public SiteStore getSiteStore() {
        return this.mSiteStore;
    }

    /**
     * Sets the profiles associated with the file server.
     *
     * @param profiles the profiles.
     */
    public void setProfiles(Profiles profiles) {
        mProfiles = profiles;
    }

    /**
     * Returns the DAGInfo that stores the metadata about the DAX
     *
     * @return
     */
    public DagInfo getDAGInfo() {
        return this.mDAGInfo;
    }

    /**
     * Generates the flow id for this current run. It is made of the name of the dag and a
     * timestamp. This is a simple concat of the mFlowTimestamp and the flowName. For it work
     * correctly the function needs to be called after the flow name and timestamp have been
     * generated.
     */
    public void generateFlowID() {
        this.mDAGInfo.generateFlowID();
    }

    /**
     * Returns the flow ID for the workflow.
     *
     * @return
     */
    public String getFlowID() {
        return mDAGInfo.getFlowID();
    }

    /**
     * Generates the name of the flow. It is same as the mNameOfADag if specified in dax generated
     * by Chimera.
     */
    public void generateFlowName() {
        this.mDAGInfo.generateFlowName();
    }

    /** Returns the flow name */
    public String getFlowName() {
        return this.mDAGInfo.getFlowName();
    }

    /**
     * Sets the dax version
     *
     * @param version the version of the DAX
     */
    public void setDAXVersion(String version) {
        this.mDAGInfo.setDAXVersion(version);
    }

    /**
     * Sets the mtime (last modified time) for the DAX. It is the time, when the DAX file was last
     * modified. If the DAX file does not exist or an IO error occurs, the MTime is set to OL i.e .
     * The DAX mTime is always generated in an extended format. Generating not in extended format,
     * leads to the XML parser tripping while parsing the invocation record generated by Kickstart.
     *
     * @param f the file descriptor to the DAX|PDAX file.
     */
    public void setDAXMTime(File f) {
        this.mDAGInfo.setDAXMTime(f);
    }

    /** Return the release version */
    public String getReleaseVersion() {
        return mDAGInfo.getReleaseVersion();
    }

    /** Grabs the release version from VDS.Properties file. */
    public void setReleaseVersion() {
        this.mDAGInfo.setReleaseVersion();
    }

    /**
     * Returns the flow timestamp for the workflow.
     *
     * @return the flowtimestamp
     */
    public String getFlowTimestamp() {
        return this.mDAGInfo.getFlowTimestamp();
    }

    /**
     * Sets the flow timestamp for the workflow.
     *
     * @param timestamp the flowtimestamp
     */
    public void setFlowTimestamp(String timestamp) {
        this.mDAGInfo.setFlowTimestamp(timestamp);
    }

    /**
     * Returns the name of the file on the basis of the metadata associated with the DAG.In case of
     * Condor dagman, it is the name of the .dag file that is written out.
     *
     * <p>The basename of the .dag file is dependant on whether the basename prefix has been
     * specified at runtime or not by command line options.
     *
     * @param options
     * @param suffix the suffix to be applied at the end.
     * @return the name of the dagfile.
     */
    protected String getDAGFilename(PlannerOptions options, String suffix) {
        return getDAGFilename(options, this.getLabel(), this.getIndex(), suffix);
    }

    /**
     * Returns the name of the file on the basis of the metadata associated with the DAG. In case of
     * Condor dagman, it is the name of the .dag file that is written out. The basename of the .dag
     * file is dependant on whether the basename prefix has been specified at runtime or not by
     * command line options.
     *
     * @param options the options passed to the planner.
     * @param name the name attribute in dax
     * @param index the index attribute in dax.
     * @param suffix the suffix to be applied at the end.
     * @return the name of the dagfile.
     */
    public static String getDAGFilename(
            PlannerOptions options, String name, String index, String suffix) {
        // constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();
        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
        } else {
            // generate the prefix from the name of the dag
            sb.append(name).append("-").append(index);
        }
        // append the suffix
        sb.append(suffix);

        return sb.toString();
    }

    /**
     * It determines the root Nodes for the ADag looking at the relation pairs of the adag. The way
     * the structure of Dag is specified in terms of the parent child relationship pairs, the
     * determination of the leaf nodes can be computationally intensive. The complexity if of order
     * n^2.
     *
     * @return the root jobs of the Adag
     * @see org.griphyn.cPlanner.classes.PCRelation
     * @see org.griphyn.cPlanner.classes.DagInfo#relations
     */
    /*    public Vector getRootNodes(){
            return this.mDAGInfo.getRootNodes();
        }
    */

    /**
     * Returns an iterator for traversing through the jobs in the workflow.
     *
     * @return a bative java failsafe iterator to the underlying collection.
     */
    public Iterator<GraphNode> jobIterator() {
        return this.nodeIterator();
        //        return this.vJobSubInfos.iterator();
    }

    /**
     * Returns the metrics about the workflow.
     *
     * @return the WorkflowMetrics
     */
    public WorkflowMetrics getWorkflowMetrics() {
        return this.mDAGInfo.getWorkflowMetrics();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing the workflow.
     *
     * @return String containing the Partition object in XML.
     * @exception IOException if something fishy happens to the stream.
     */
    public String toDOT() throws IOException {
        Writer writer = new StringWriter(32);
        toDOT(writer, "");
        return writer.toString();
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing the workflow.
     *
     * @param stream is a stream opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toDOT(Writer stream, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        String newIndent = (indent == null) ? "\t" : indent + "\t";

        // write out the dot header
        writeDOTHeader(stream, null);

        // traverse through the jobs
        for (Iterator it = jobIterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            ((Job) node.getContent()).toDOT(stream, newIndent);
        }

        stream.write(newLine);

        for (Iterator<GraphNode> it = this.jobIterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();

            // get a list of parents of the node
            for (GraphNode child : gn.getChildren()) {
                this.edgeToDOT(stream, newIndent, gn.getID(), child.getID());
            }
        }

        // write out the tail
        stream.write("}");
        stream.write(newLine);
    }

    /**
     * Returns the DOT description of the object. This is used for visualizing the workflow.
     *
     * @param stream is a stream opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param parent the parent
     * @param child the child
     * @exception IOException if something fishy happens to the stream.
     */
    private void edgeToDOT(Writer stream, String indent, String parent, String child)
            throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        // write out the edge
        stream.write(indent);
        stream.write("\"");
        stream.write(parent);
        stream.write("\"");
        stream.write(" -> ");
        stream.write("\"");
        stream.write(child);
        stream.write("\"");
        stream.write(newLine);
        stream.flush();
    }

    /**
     * Writes out the static DOT Header.
     *
     * @param stream is a stream opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeDOTHeader(Writer stream, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        String newIndent = (indent == null) ? null : indent + "\t";

        // write out the header and static stuff for now
        if (indent != null && indent.length() > 0) {
            stream.write(indent);
        }
        stream.write("digraph E {");
        stream.write(newLine);

        // write out the size of the image
        if (newIndent != null && newIndent.length() > 0) {
            stream.write(newIndent);
        }
        stream.write("size=\"8.0,10.0\"");
        stream.write(newLine);

        // write out the ratio
        if (newIndent != null && newIndent.length() > 0) {
            stream.write(newIndent);
        }
        stream.write("ratio=fill");
        stream.write(newLine);

        // write out what the shape of the nodes need to be like
        if (newIndent != null && newIndent.length() > 0) {
            stream.write(newIndent);
        }
        stream.write("node [shape=ellipse]");
        stream.write(newLine);

        // write out how edges are to be rendered.
        if (newIndent != null && newIndent.length() > 0) {
            stream.write(newIndent);
        }
        stream.write("edge [arrowhead=normal, arrowsize=1.0]");
        stream.write(newLine);
    }

    /**
     * Adds a node to the Graph. It overwrites an already existing node with the same ID.
     *
     * @param node the node to be added to the Graph.
     */
    public void addNode(GraphNode node) {
        this.mGraphImplementor.addNode(node);
        // increment associated workflow metrics
        this.mDAGInfo.getWorkflowMetrics().increment((Job) node.getContent());
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
    public void addEdges(String child, List<String> parents) {
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
        GraphNode node = this.getNode(identifier);
        if (node != null) {
            this.mDAGInfo.getWorkflowMetrics().decrement((Job) node.getContent());
        }
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
    public Iterator<GraphNode> bottomUpIterator() {
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

    /** @param node */
    public void setGraphNodeReference(GraphNode node) {
        throw new UnsupportedOperationException("GraphNode reference not set for ADag");
    }
}
