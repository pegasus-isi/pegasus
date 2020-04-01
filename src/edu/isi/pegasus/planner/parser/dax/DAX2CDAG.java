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
package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.DagInfo;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.classes.WorkflowMetrics;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.parser.XMLParser;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This creates a dag corresponding to one particular partition of the whole abstract plan. The
 * partition can be as big as the whole abstract graph or can be as small as a single job. The
 * partitions are determined by the Partitioner.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAX2CDAG implements Callback {

    /** The ADag object which contains information corresponding to the ADag in the XML file. */
    private ADag mDag;

    /** The mapping of the idrefs of a job to the job name. e.g ID0000001 -> preprocess_ID000001 */
    private Map<String, String> mJobMap;

    /** The handle to the properties object. */
    private PegasusProperties mProps;

    /** A flag to specify whether the graph has been generated for the partition or not. */
    private boolean mDone;

    /** Handle to the replica store that stores the replica catalog user specifies in the DAX */
    protected ReplicaStore mReplicaStore;

    /**
     * Handle to the transformation store that stores the transformation catalog user specifies in
     * the DAX
     */
    protected TransformationStore mTransformationStore;

    /** Map of Compound Transformations indexed by complete name of the compound transformation. */
    protected Map<String, CompoundTransformation> mCompoundTransformations;

    /** All the notifications associated with the adag */
    private Notifications mNotifications;

    /** To track whether we auto detect data dependancies or not. */
    private boolean mAddDataDependencies;

    /** Map to track a LFN with the job that creates the file corresponding to the LFN. */
    private Map<String, Job> mFileCreationMap;

    /** The handle to the logger */
    private LogManager mLogger;

    /** A job prefix specifed at command line. */
    protected String mJobPrefix;

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects containing the properties and the logger
     * @param dax the path to the DAX file.
     */
    public void initialize(PegasusBag bag, String dax) {
        mDag = new ADag();
        mJobMap = new HashMap<String, String>();
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mDone = false;
        this.mJobPrefix =
                (bag.getPlannerOptions() == null)
                        ? null
                        : bag.getPlannerOptions().getJobnamePrefix();
        this.mReplicaStore = new ReplicaStore();
        this.mTransformationStore = new TransformationStore();
        this.mCompoundTransformations = new HashMap<String, CompoundTransformation>();
        this.mNotifications = new Notifications();
        this.mAddDataDependencies = mProps.addDataDependencies();
        this.mFileCreationMap = new HashMap<String, Job>();
    }

    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. It ends up storing the attributes with the adag element in the internal memory
     * structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        mDag.setDAXVersion((String) attributes.get("version"));
        mDag.setCount((String) attributes.get("count"));
        mDag.setIndex((String) attributes.get("index"));
        mDag.setLabel((String) attributes.get("name"));
    }

    /**
     * Callback when a invoke entry is encountered in the top level inside the adag element in DAX.
     *
     * @param invoke the invoke object
     */
    public void cbWfInvoke(Invoke invoke) {
        // System.out.println( "[DEBUG] WF Invoke " + invoke );
        this.mNotifications.add(invoke);
    }

    /**
     * Callback when a metadata element is encountered in the adag element.
     *
     * @param profile profile element of namespace metadata
     */
    public void cbMetadata(Profile p) {
        this.mDag.addMetadata(p.getProfileKey(), p.getProfileValue());
    }

    /**
     * Callback for the job from section 2 jobs. These jobs are completely assembled, but each is
     * passed separately.
     *
     * @param job the <code>Job</code> object storing the job information gotten from parser.
     */
    public void cbJob(Job job) {
        // PM-1501 first compute derived attributes
        // set the internal primary id for job
        if (job instanceof DAXJob) {
            // set the internal primary id for job
            // daxJob.setName( constructJobID( daxJob ) );
            job.setName(
                    XMLParser.makeDAGManCompliant(((DAXJob) job).generateName(this.mJobPrefix)));
        } else if (job instanceof DAGJob) {
            job.setName(
                    XMLParser.makeDAGManCompliant(((DAGJob) job).generateName(this.mJobPrefix)));
        }
        job.setName(constructJobID(job));
        // all jobs deserialized are of type compute
        job.setJobType(Job.COMPUTE_JOB);

        mJobMap.put(job.logicalId, job.jobName);
        mDag.add(job);

        DagInfo dinfo = mDag.getDAGInfo();

        // check for compound executables
        if (this.mCompoundTransformations.containsKey(job.getCompleteTCName())) {
            CompoundTransformation ct = this.mCompoundTransformations.get(job.getCompleteTCName());
            // add all the dependant executables and data files
            for (PegasusFile pf : ct.getDependantFiles()) {
                job.addInputFile(pf);
                String lfn = pf.getLFN();
                dinfo.updateLFNMap(lfn, "i");
            }
            job.addNotifications(ct.getNotifications());
        }

        // put the input files in the map
        for (Iterator it = job.inputFiles.iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();
            dinfo.updateLFNMap(lfn, "i");
        }

        for (Iterator it = job.outputFiles.iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = (pf).getLFN();

            // if the output LFN is also an input LFN of the same
            // job then it is a pass through LFN. Should be tagged
            // as i only, as we want it staged in\
            if (job.inputFiles.contains(pf)) {
                // PM-1253 explicitly complain instead of silently allowing this
                // only allow for for checkpoint files, that
                // are flagged as both input and output
                if (pf.isCheckpointFile()) {
                    continue;
                }
                throw new RuntimeException(
                        "File " + lfn + " is listed as input and output for job " + job.getID());
            }
            dinfo.updateLFNMap(lfn, "o");
            if (this.mAddDataDependencies) {
                mFileCreationMap.put(lfn, job);
            }
        }
    }

    /**
     * Callback for child and parentID relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, List<PCRelation> parents) {
        String childID = (String) mJobMap.get(child);
        String parentID;

        if (childID == null) {
            throw new RuntimeException("Unable to find job in DAX with ID " + child);
        }
        // System.out.println( child + " -> " + parents );

        for (PCRelation pc : parents) {
            parentID = (String) mJobMap.get(pc.getParent());
            if (parentID == null) {
                // this actually means dax is generated wrong.
                // probably some one tinkered with it by hand.
                throw new RuntimeException(
                        "Unable to find job in DAX with ID "
                                + pc.getParent()
                                + " listed as a parent for job with ID "
                                + child);
            }

            /* PM-747
            PCRelation relation = new PCRelation( parentID, childID  );
            relation.setAbstractChildID( child );
            relation.setAbstractParentID( pc.getParent() );
            mDagInfo.addNewRelation( relation );
            */
            mDag.addEdge(parentID, childID);
        }
    }

    /**
     * Callback for child and parent relationships from Section 5: Dependencies that lists Parent
     * Child relationships (can be empty)
     *
     * @param parent is the IDREF of the child element.
     * @param children is a list of id's of children nodes.
     */
    public void cbChildren(String parent, List<String> children) {
        String parentID = (String) mJobMap.get(parent);
        String childID;

        if (parentID == null) {
            throw new RuntimeException("Unable to find job in DAX with ID " + parent);
        }
        for (String child : children) {
            childID = (String) mJobMap.get(child);
            if (childID == null) {
                // this actually means dax is generated wrong.
                // probably some one tinkered with it by hand.
                throw new RuntimeException(
                        "Unable to find job in DAX with ID "
                                + child
                                + " listed as a child for job with ID "
                                + parent);
            }
            mDag.addEdge(parentID, childID);
        }
    }

    /**
     * Callback when the parsing of the document is done. It sets the flag that the parsing has been
     * done, that is used to determine whether the ADag object has been fully generated or not.
     */
    public void cbDone() {
        mDone = true;

        // compute some file count metrics and set them
        WorkflowMetrics fileMetrics = this.mDag.getDAGInfo().computeDAXFileCounts();
        WorkflowMetrics metrics = this.mDag.getWorkflowMetrics();
        metrics.setNumDAXFiles(
                WorkflowMetrics.FILE_TYPE.input,
                fileMetrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input));
        metrics.setNumDAXFiles(
                WorkflowMetrics.FILE_TYPE.intermediate,
                fileMetrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate));
        metrics.setNumDAXFiles(
                WorkflowMetrics.FILE_TYPE.output,
                fileMetrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output));
        metrics.setNumDAXFiles(
                WorkflowMetrics.FILE_TYPE.total,
                fileMetrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total));

        if (this.mAddDataDependencies) {
            this.addDataDependencies();
        }
    }

    /**
     * Returns an ADag object corresponding to the abstract plan it has generated. It throws a
     * runtime exception if the method is called before the object has been created fully.
     *
     * @return ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject() {
        if (!mDone)
            throw new RuntimeException(
                    "Method called before the abstract dag "
                            + " for the partition was fully generated");

        mDag.setReplicaStore(mReplicaStore);
        mDag.setTransformationStore(mTransformationStore);
        mDag.addNotifications(mNotifications);
        return mDag;
    }

    /**
     * Callback when a compound transformation is encountered in the DAX
     *
     * @param compoundTransformation the compound transformation
     */
    public void cbCompoundTransformation(CompoundTransformation compoundTransformation) {
        this.mCompoundTransformations.put(
                compoundTransformation.getCompleteName(), compoundTransformation);
        if (!compoundTransformation.getNotifications().isEmpty()) {
            mLogger.log(
                    "Compound Transformation Invoke "
                            + compoundTransformation.getCompleteName()
                            + " "
                            + compoundTransformation.getNotifications(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Callback when a replica catalog entry is encountered in the DAX
     *
     * @param rl the ReplicaLocation object
     */
    public void cbFile(ReplicaLocation rl) {
        // System.out.println( "File Locations passed are " + rl );
        // we only add to replica store if there is a PFN specified
        if (rl.getPFNCount() > 0) {
            this.mReplicaStore.add(rl);
        }
    }

    /**
     * Callback when a transformation catalog entry is encountered in the DAX
     *
     * @param tce the transformation catalog entry object.
     */
    public void cbExecutable(TransformationCatalogEntry tce) {
        this.mTransformationStore.addEntry(tce);
        if (!tce.getNotifications().isEmpty()) {
            mLogger.log(
                    "Executable Invoke "
                            + tce.getLogicalTransformation()
                            + " "
                            + tce.getNotifications(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Goes through the ADag and computes any data dependencies.
     *
     * <p>For example if Job A creates an output file X and job B consumes it, then it automatically
     * adds a dependency between A -> B if it does not exist already.
     */
    private void addDataDependencies() {
        mLogger.logEventStart(
                LoggingKeys.EVENT_PEGASUS_ADD_DATA_DEPENDENCIES,
                LoggingKeys.DAX_ID,
                this.mDag.getAbstractWorkflowName());
        for (Iterator<GraphNode> it = this.mDag.nodeIterator(); it.hasNext(); ) {
            GraphNode child = it.next();
            Set<GraphNode> parents = new HashSet();
            Job job = (Job) child.getContent();
            for (PegasusFile pf : job.getInputFiles()) {
                Job parent = this.mFileCreationMap.get(pf.getLFN());
                if (parent != null) {
                    parents.add(this.mDag.getNode(parent.getID()));
                }
            }
            // now add depedencies for the job
            for (GraphNode parent : parents) {
                mLogger.log(
                        "Adding Data Dependency edge " + parent.getID() + " -> " + job.getID(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
                this.mDag.addEdge(parent, child);
            }
        }
        mLogger.logEventCompletion();
    }

    /**
     * Returns the id for a job
     *
     * @param j the job
     * @return the id.
     */
    protected String constructJobID(Job j) {
        // construct the jobname/primary key for job
        StringBuilder name = new StringBuilder();

        // prepend a job prefix to job if required
        if (mJobPrefix != null) {
            name.append(mJobPrefix);
        }

        // append the name and id recevied from dax
        name.append(j.getTXName());
        name.append("_");
        name.append(j.getLogicalID());

        // PM-1222 strip out any . from transformation name
        return XMLParser.makeDAGManCompliant(name.toString());
    }
}
