/**
 * Copyright 2007-2017 University Of Southern California
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
package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.DataFlowJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.code.generator.condor.style.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;

/**
 * Decaf data flows are represented as clustered job. DECAF implementation to render the data flow
 * in the DECAF JSON.
 *
 * @author Karan Vahi
 */
public class Decaf extends Abstract {

    /** The key indicating the number of processors to run the job on. */
    private static final String NPROCS_KEY = "nprocs";

    /** The base submit directory for the workflow. */
    protected String mWFSubmitDirectory;

    private ADag mDAG;

    public void initialize(ADag dag, PegasusBag bag) {
        super.initialize(dag, bag);
        this.mDAG = dag;
        mWFSubmitDirectory = bag.getPlannerOptions().getSubmitDirectory();
    }

    /**
     * Constructs a new aggregated job that contains all the jobs passed to it. The new aggregated
     * job, appears as a single job in the workflow and replaces the jobs it contains in the
     * workflow.
     *
     * @param jobs the list of <code>Job</code> objects that need to be collapsed. All the jobs
     *     being collapsed should be scheduled at the same pool, to maintain correct semantics.
     * @param name the logical name of the jobs in the list passed to this function.
     * @param id the id that is given to the new job.
     * @return the <code>Job</code> object corresponding to the aggregated job containing the jobs
     *     passed as List in the input, null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob(List jobs, String name, String id) {
        // create a normal clustered aggregated job and then convert it to a DataFlowJob
        AggregatedJob clusteredJob = super.constructAbstractAggregatedJob(jobs, name, id);
        // PM-1798 at this point we only have clustered job with the jobs making them up
        // but no edges between the constituent jobs. those are visible only when
        // the clustered job is made concrete
        // This function is called only if we are getting Pegasus to cluster jobs into a
        // DECAF job. For the case, where the workflow description has a data flow job,
        // the data flow job is already there and does not go through job clustering
        return this.createPartialDataFlowJob(clusteredJob);
    }

    /**
     * Enables the abstract clustered job for execution and converts it to it's executable form
     *
     * @param job the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete(AggregatedJob job) {
        if (!(job instanceof DataFlowJob)) {
            // sanity check
            throw new RuntimeException(
                    "Decaf job aggregator requires jobs of type DataFlowJob for job" + job.getID());
        }

        DataFlowJob j = (DataFlowJob) job;
        if (j.isPartiallyCreated()) {
            this.addLinksToDataFlowJob(j);
            j.setPartiallyCreated(false);

            // for root jobs in the clustered job  we have no inports
            // and for leaf jobs in the clustered job we have no outports
            for (GraphNode node : job.getRoots()) {
                Job root = (Job) node.getContent();
                Namespace decafAttributes = root.getSelectorProfiles();
                decafAttributes.construct("inports", "");
            }

            for (GraphNode node : job.getLeaves()) {
                Job root = (Job) node.getContent();
                Namespace decafAttributes = root.getSelectorProfiles();
                decafAttributes.construct("outports", "");
            }
        }

        this.makeAbstractAggregatedJobConcrete(j);
    }

    /**
     * Converts a DataFlowJob to the corresponding DECAF json description.
     *
     * @param job the abstract clustered job as a data flow job
     */
    public void makeAbstractAggregatedJobConcrete(DataFlowJob job) {

        // figure out name and directory
        // String name = job.getID() + ".json";

        // we cannot give any name because of hardcoded nature
        // in decaf. instead pick up the name as defined in the
        // transformation catalog for dataflow::decaf
        String dataFlowExecutable = job.getRemoteExecutable();
        String name = new File(dataFlowExecutable).getName();
        if (!name.endsWith(".json")) {
            throw new RuntimeException(
                    "Data flow job with id "
                            + job.getID()
                            + " should be mapped to a json file in Transformation Catalog. Is mapped to "
                            + name);
        }

        // traverse through the nodes making up the Data flow job
        // and update resource requirements
        int cores = 0;
        for (Iterator it = job.nodeIterator(); it.hasNext(); ) {
            GraphNode n = (GraphNode) it.next();
            Job j = (Job) n.getContent();
            Namespace decafProfiles = j.getSelectorProfiles();
            if (decafProfiles.containsKey(Decaf.NPROCS_KEY)) {
                j.vdsNS.construct(Pegasus.CORES_KEY, (String) decafProfiles.get(Decaf.NPROCS_KEY));
            }
            int c = j.vdsNS.getIntValue(Pegasus.CORES_KEY, -1);
            if (c == -1) {
                throw new RuntimeException(
                        "Invalid number of cores or decaf key "
                                + Decaf.NPROCS_KEY
                                + " specified for job "
                                + j.getID());
            }
            cores += c;
        }
        job.vdsNS.construct(Pegasus.CORES_KEY, Integer.toString(cores));

        // PM-833 the .in file should be in the same directory where all job submit files go
        File directory = new File(this.mWFSubmitDirectory, job.getRelativeSubmitDirectory());
        File jsonFile = new File(directory, name);
        File launchFile = new File(directory, job.getID() + ".sh");
        Writer jsonWriter = getWriter(jsonFile);
        Writer launchWriter = getWriter(launchFile);

        // generate the json file for the data flow job
        writeOutDECAFJsonWorkflow((DataFlowJob) job, jsonWriter);
        job.condorVariables.addIPFileForTransfer(jsonFile.getAbsolutePath());

        // generate the shell script that sets up the MPMD invocation
        writeOutLaunchScript((DataFlowJob) job, launchWriter);

        // set the xbit to true for the file
        launchFile.setExecutable(true);
        // the launch file is the main executable for the data flow
        job.setRemoteExecutable(launchFile.toString());
        // rely on condor to transfer the script to the remote cluster
        job.condorVariables.construct(Condor.TRANSFER_EXECUTABLE_KEY, "true");
    }

    /**
     * A boolean indicating whether ordering is important while traversing through the aggregated
     * job.
     *
     * @return a boolean
     */
    public boolean topologicalOrderingRequired() {
        // we don't care about ordering, and decaf jobs can have cycles
        return false;
    }

    /**
     * Setter method to indicate , failure on first consitutent job should result in the abort of
     * the whole aggregated job.
     *
     * @param fail indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure(boolean fail) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on detecting the first
     * failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure() {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
    }

    /**
     * Determines whether there is NOT an entry in the transformation catalog for the job aggregator
     * executable on a particular site.
     *
     * @param site the site at which existence check is required.
     * @return boolean true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site) {
        // PM-1798 decaf does not map to a clustering executable
        // however return a false to allow for planning
        return false;
        // Templates.
    }

    /**
     * Returns the logical name of the transformation that is used to collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     */
    public String getClusterExecutableLFN() {
        // return random basename
        return "decaf";
    }

    /**
     * Returns the executable basename of the clustering executable used.
     *
     * @return the executable basename.
     */
    public String getClusterExecutableBasename() {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
    }

    /**
     * Writes out the data flow as decaf workflow represented as JSON. Sample DECAF 2 node workflow
     * represented here.
     *
     * <pre>
     * {
     * "workflow": {
     * "filter_level": "NONE",
     * "nodes": [
     * {
     * "nprocs": 4,
     * "start_proc": 0,
     * "func": "prod"
     * },
     * {
     * "nprocs": 2,
     * "start_proc": 6,
     * "func": "con"
     * }
     * ],
     * "edges": [
     * {
     * "nprocs": 2,
     * "start_proc": 4,
     * "source": 0,
     * "func": "dflow",
     * "prod_dflow_redist": "count",
     * "path": "/data/scratch/vahi/software/install/decaf/default/examples/direct/mod_linear_2nodes.so",
     * "dflow_con_redist": "count",
     * "target": 1
     * }
     * ]
     * }
     * }
     * </pre>
     *
     * @param dataFlowJob
     * @param writer
     */
    private void writeOutDECAFJsonWorkflow(DataFlowJob job, Writer writer) {
        Map<String, Object> properties = new HashMap<String, Object>(1);
        properties.put(JsonGenerator.PRETTY_PRINTING, true);
        JsonGeneratorFactory factory = Json.createGeneratorFactory(properties);
        JsonGenerator generator = factory.createGenerator(writer);

        // separate out the nodes and the link jobs from the collection.
        // linked list is important as order determine the source and target
        // in links
        LinkedList<Job> nodes = new LinkedList();
        LinkedList<DataFlowJob.Link> links = new LinkedList();
        // maps tasks logical id to index in the nodes array of json
        Map<String, Integer> logicalIDToIndex = new HashMap();
        // int index = 0;
        for (Iterator it = job.nodeIterator(); it.hasNext(); ) {
            GraphNode n = (GraphNode) it.next();
            Job j = (Job) n.getContent();
            if (j instanceof DataFlowJob.Link) {
                links.add((DataFlowJob.Link) j);
            } else {
                // logicalIDToIndex.put(j.getLogicalID(), index++);
                // pick up the id from the selector namespace
                String id = (String) j.getSelectorProfiles().get("id");
                if (id == null) {
                    throw new RuntimeException(
                            "decaf id not associated for the job " + j.getLogicalID());
                }
                logicalIDToIndex.put(j.getLogicalID(), Integer.parseInt(id));
                nodes.add(j);
            }
        }

        generator.writeStartObject();
        generator.writeStartObject("workflow").write("filter_level", "NONE");
        generator.writeStartArray("nodes");
        for (Job j : nodes) {
            // decaf attributes are stored as selector profiles
            Namespace decafAttrs = j.getSelectorProfiles();
            // PM-1794 inject job args to cmdline parameters
            decafAttrs.construct("cmdline", j.getRemoteExecutable() + " " + j.getArguments());

            generator.writeStartObject();
            for (Iterator profileIt = decafAttrs.getProfileKeyIterator(); profileIt.hasNext(); ) {
                String key = (String) profileIt.next();
                String value = (String) decafAttrs.get(key);
                // check for int values
                Integer v = -1;
                try {
                    v = Integer.parseInt(value);
                } catch (Exception e) {
                }

                if (v == -1) {
                    generator.write(key, value);

                } else {
                    generator.write(key, v);
                }
            }
            generator.writeEnd();
        }
        generator.writeEnd(); // for nodes

        // System.err.println(logicalIDToIndex);
        generator.writeStartArray("edges");
        for (DataFlowJob.Link j : links) {
            // decaf attributes are stored as selector profiles
            Namespace decafAttrs = j.getSelectorProfiles();
            generator.writeStartObject();

            int source = logicalIDToIndex.get(j.getParentID());
            int target = logicalIDToIndex.get(j.getChildID());
            // write out source and target
            generator.write("source", source);
            generator.write("target", target);

            StringBuilder sb = new StringBuilder();
            sb.append("Translated link edge to source target")
                    .append(" ")
                    .append(j.getParentID())
                    .append("->")
                    .append(j.getChildID());
            sb.append(" ")
                    .append(" to source,target")
                    .append(" ")
                    .append(source)
                    .append("->")
                    .append(target);
            mLogger.log(sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
            for (Iterator profileIt = decafAttrs.getProfileKeyIterator(); profileIt.hasNext(); ) {
                String key = (String) profileIt.next();
                String value = (String) decafAttrs.get(key);

                // check for int values
                Integer v = -1;
                try {
                    v = Integer.parseInt(value);
                } catch (Exception e) {
                }

                if (v == -1) {
                    generator.write(key, value);

                } else {
                    generator.write(key, v);
                }
            }
            generator.writeEnd();
        }
        generator.writeEnd(); // for nodes

        generator.writeEnd(); // for workflow
        generator.writeEnd(); // for document

        generator.close();
    }

    /**
     * Writer to write out the launch script for the data flow job.
     *
     * @param job
     * @param writer
     */
    private void writeOutLaunchScript(DataFlowJob job, Writer writer) {
        PrintWriter pw = new PrintWriter(writer);
        pw.println("#!/bin/bash");
        pw.println("set -e");

        // PM-1792 ensure that the job is launched from PEGASUS_SCRATCH_DIR
        // PEGASUS_SCRATCH_DIR is always set as an environment variable in
        // generated condor submit file
        pw.println("cd $" + ENV.PEGASUS_SCRATCH_DIR_KEY);

        pw.println("echo \" Launched from directory `pwd` \" ");

        // mpirun  -np 4 ./linear_2nodes : -np 2 ./linear_2nodes : -np 2 ./linear_2nodes
        StringBuilder sb = new StringBuilder();
        sb.append("mpirun").append(" ");
        // traverse through the nodes making up the Data flow job
        // and update resource requirements
        boolean first = true;
        for (Iterator it = job.nodeIterator(); it.hasNext(); ) {
            GraphNode n = (GraphNode) it.next();
            Job j = (Job) n.getContent();
            int cores = j.vdsNS.getIntValue(Pegasus.CORES_KEY, -1);

            if (cores == 0) {
                if (j instanceof DataFlowJob.Link) {
                    // PM-1602 skip the job in the mpirun invocation
                    mLogger.log(
                            "Skipping data link job for invocation by mpirun as number of cores is 0 - "
                                    + j.getLogicalID(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                    continue;
                }
                // log warning for non link job
                mLogger.log(
                        "Number of cores is 0 for job " + j.getLogicalID(),
                        LogManager.WARNING_MESSAGE_LEVEL);
            }

            if (!first) {
                sb.append(" ").append(":").append(" ");
            }
            sb.append("-np").append(" ").append(cores).append(" ").append(j.getRemoteExecutable());

            String args = j.getArguments();
            if (args != null && args.length() > 0) {
                // PM-1602 set arguments associated with the job
                sb.append(" ").append(args);
            }
            first = false;
        }
        pw.println(sb);
        pw.close();
    }

    private Writer getWriter(File jsonFile) {
        Writer writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(jsonFile));
        } catch (IOException ex) {
            throw new RuntimeException("Unable to open file " + jsonFile + " for writing ", ex);
        }
        return writer;
    }

    /**
     * An adapter function that converts a Pegasus clustered job to a DataFlowJob that can be
     * executed using Decaf
     *
     * @param job the job to be converted
     * @return DataFlowJob
     */
    private DataFlowJob createPartialDataFlowJob(AggregatedJob job) {
        DataFlowJob d = new DataFlowJob();

        // pick some things from aggregated job
        d.setTXName(job.getTXName());
        d.setRemoteExecutable(job.getName() + ".json");
        d.setName(job.getID());
        d.setRelativeSubmitDirectory(job.getRelativeSubmitDirectory());
        d.setArguments(job.getArguments());
        d.setJobType(Job.COMPUTE_JOB);
        d.setSiteHandle(job.getSiteHandle());
        d.setStagingSiteHandle(job.getStagingSiteHandle());
        d.setJobAggregator(job.getJobAggregator());
        int taskid = 0;
        for (Iterator<GraphNode> it = job.nodeIterator(); it.hasNext(); taskid++) {
            GraphNode node = it.next();
            Job constitutentJob = (Job) node.getContent();
            mLogger.log(
                    "Assigning decaf id " + taskid + " to job " + constitutentJob.getID(),
                    LogManager.DEBUG_MESSAGE_LEVEL);

            // each job in the cluster run on it's own proc
            // make start_proc same as taskid
            constitutentJob.addProfile(new Profile("selector", "id", Integer.toString(taskid)));
            constitutentJob.addProfile(
                    new Profile("selector", "start_proc", Integer.toString(taskid)));
            // each job runs on a single proc
            constitutentJob.addProfile(new Profile("selector", "nprocs", "1"));
            constitutentJob.addProfile(new Profile("selector", "inports", "in"));
            constitutentJob.addProfile(new Profile("selector", "outports", "out"));
            // func is tricky. just map it to job transformation id with taskid
            constitutentJob.addProfile(
                    new Profile("selector", "func", constitutentJob.getTXName() + taskid));
            d.add(constitutentJob);
        }

        d.setPartiallyCreated();

        return d;
    }

    @Override
    public String aggregatedJobArguments(AggregatedJob job) {
        return "";
    }

    /**
     * Adds LinkJobs to the job corresponding to the edges in the DAG
     *
     * @param job the data flow job
     */
    private void addLinksToDataFlowJob(DataFlowJob job) {
        int jobNum = job.size();
        // lets write out the edges
        List<DataFlowJob.Link> linkJobs = new LinkedList();
        for (Iterator<GraphNode> it = job.nodeIterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();
            Job parentJob = (Job) gn.getContent();

            // get a list of parents of the node
            for (GraphNode child : gn.getChildren()) {
                Job childJob = (Job) child.getContent();
                StringBuffer edge = new StringBuffer();
                edge.append("Creating a LinkJob for EDGE")
                        .append(" ")
                        .append(gn.getID())
                        .append(" ")
                        .append(child.getID());
                mLogger.log(edge.toString(), LogManager.DEBUG_MESSAGE_LEVEL);

                DataFlowJob.Link link = new DataFlowJob.Link();

                // set the logical id to be the current number of jobs in DataFlowJob
                link.setLogicalID("ID" + ++jobNum);
                link.setUniverse(GridGateway.JOB_TYPE.compute.toString());
                link.setJobType(Job.COMPUTE_JOB);
                link.setSiteHandle(parentJob.getSiteHandle());
                link.setStagingSiteHandle(parentJob.getStagingSiteHandle());

                // generates source and target attributes
                link.setLink(gn.getID(), child.getID());

                // some hardcoded stuff for time being
                /**
                 * "start_proc": 0, "nprocs": 0, "prod_dflow_redist": "count", "name":
                 * "interm1_interm2", "sourcePort": "out", "targetPort": "in", "tokens": 0,
                 * "transport": "mpi", "func": "dflow", "dflow_con_redist": "count"
                 */

                // we are running link jobs on zero cores?
                Namespace decafKeys = link.getSelectorProfiles();
                decafKeys.construct("start_proc", "0");
                decafKeys.construct(Decaf.NPROCS_KEY, "0");
                decafKeys.construct("tokens", "0");
                decafKeys.construct("transport", "mpi");
                decafKeys.construct("func", "dflow");
                decafKeys.construct("prod_dflow_redist", "count");
                decafKeys.construct("dflow_con_redist", "count");
                decafKeys.construct("sourcePort", "out");
                decafKeys.construct("targetPort", "in");

                // make the name based on edge in question based on func attribute
                // for the parent and child
                String name = getDecafFunc(parentJob) + "_" + getDecafFunc(childJob);
                decafKeys.construct("name", name);
                job.addEdge(link);
                linkJobs.add(link);
            }
        }

        // add all the data link jobs to the DataFlowJob
        for (DataFlowJob.Link link : linkJobs) {
            job.add(link);
        }
    }

    /**
     * Returns the decaf id for the job
     *
     * @return the decaf id (usually an int)
     */
    private String getDecafJobID(Job j) {
        String id = (String) j.getSelectorProfiles().get("id");
        if (id == null) {
            throw new RuntimeException("Job not associated with a decaf int id " + j.getID());
        }
        return id;
    }

    /**
     * Returns the decaf id for the job
     *
     * @return the decaf id (usually an int)
     */
    private String getDecafFunc(Job j) {
        String id = (String) j.getSelectorProfiles().get("func");
        if (id == null) {
            throw new RuntimeException(
                    "Job not associated with a decaf func attribute " + j.getID());
        }
        return id;
    }
}
