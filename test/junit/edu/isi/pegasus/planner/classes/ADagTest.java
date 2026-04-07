/**
 * Copyright 2007-2013 University Of Southern California
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the ADag class. */
public class ADagTest {

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultConstructorProducesEmptyWorkflow() {
        ADag dag = new ADag();
        assertThat(dag.getNoOfJobs(), is(0));
        assertThat(dag.isEmpty(), is(true));
    }

    @Test
    public void testDefaultConstructorSetsSubmitDirectory() {
        ADag dag = new ADag();
        assertThat(dag.getBaseSubmitDirectory(), is("."));
    }

    @Test
    public void testDefaultConstructorGeneratesWorkflowUUID() {
        ADag dag = new ADag();
        assertThat(dag.getWorkflowUUID(), is(notNullValue()));
        assertThat(dag.getWorkflowUUID().length(), greaterThan(0));
    }

    @Test
    public void testDefaultConstructorRootWorkflowUUIDIsNull() {
        ADag dag = new ADag();
        assertThat(dag.getRootWorkflowUUID(), is(nullValue()));
    }

    @Test
    public void testDefaultConstructorRefinementNotStarted() {
        ADag dag = new ADag();
        assertThat(dag.hasWorkflowRefinementStarted(), is(false));
    }

    // -----------------------------------------------------------------------
    // Label / Index / Count / UUID setters and getters
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetLabel() {
        ADag dag = new ADag();
        dag.setLabel("my-workflow");
        assertThat(dag.getLabel(), is("my-workflow"));
    }

    @Test
    public void testSetAndGetIndex() {
        ADag dag = new ADag();
        dag.setIndex("3");
        assertThat(dag.getIndex(), is("3"));
    }

    @Test
    public void testSetAndGetCount() {
        ADag dag = new ADag();
        dag.setCount("10");
        assertThat(dag.getCount(), is("10"));
    }

    @Test
    public void testSetAndGetWorkflowUUID() {
        ADag dag = new ADag();
        dag.setWorkflowUUID("abc-123");
        assertThat(dag.getWorkflowUUID(), is("abc-123"));
    }

    @Test
    public void testSetAndGetRootWorkflowUUID() {
        ADag dag = new ADag();
        dag.setRootWorkflowUUID("root-uuid-999");
        assertThat(dag.getRootWorkflowUUID(), is("root-uuid-999"));
    }

    @Test
    public void testSetAndGetBaseSubmitDirectory() {
        ADag dag = new ADag();
        dag.setBaseSubmitDirectory("/tmp/submit");
        assertThat(dag.getBaseSubmitDirectory(), is("/tmp/submit"));
    }

    @Test
    public void testSetAndGetRequestID() {
        ADag dag = new ADag();
        dag.setRequestID("req-42");
        assertThat(dag.getRequestID(), is("req-42"));
    }

    @Test
    public void testWorkflowRefinementStartedToggle() {
        ADag dag = new ADag();
        assertThat(dag.hasWorkflowRefinementStarted(), is(false));
        dag.setWorkflowRefinementStarted(true);
        assertThat(dag.hasWorkflowRefinementStarted(), is(true));
        dag.setWorkflowRefinementStarted(false);
        assertThat(dag.hasWorkflowRefinementStarted(), is(false));
    }

    // -----------------------------------------------------------------------
    // Workflow name helpers
    // -----------------------------------------------------------------------

    @Test
    public void testGetAbstractWorkflowName() {
        ADag dag = new ADag();
        dag.setLabel("blackdiamond");
        dag.setIndex("0");
        assertThat(dag.getAbstractWorkflowName(), is("blackdiamond-0"));
    }

    @Test
    public void testGetExecutableWorkflowName() {
        ADag dag = new ADag();
        dag.setLabel("blackdiamond");
        dag.setIndex("0");
        assertThat(dag.getExecutableWorkflowName(), is("blackdiamond_0.dag"));
    }

    // -----------------------------------------------------------------------
    // Adding and counting jobs
    // -----------------------------------------------------------------------

    @Test
    public void testAddSingleJob() {
        ADag dag = new ADag();
        Job job = createJob("job1");
        dag.add(job);
        assertThat(dag.getNoOfJobs(), is(1));
        assertThat(dag.isEmpty(), is(false));
    }

    @Test
    public void testAddMultipleJobs() {
        ADag dag = new ADag();
        dag.add(createJob("jobA"));
        dag.add(createJob("jobB"));
        dag.add(createJob("jobC"));
        assertThat(dag.getNoOfJobs(), is(3));
    }

    @Test
    public void testGetNodeAfterAdd() {
        ADag dag = new ADag();
        Job job = createJob("findJob");
        dag.add(job);
        GraphNode node = dag.getNode("findJob");
        assertThat(node, is(notNullValue()));
        assertThat(node.getID(), is("findJob"));
    }

    @Test
    public void testGetNodeForMissingIDReturnsNull() {
        ADag dag = new ADag();
        assertThat(dag.getNode("doesNotExist"), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // Removing jobs
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveJobByObject() {
        ADag dag = new ADag();
        Job job = createJob("removeMe");
        dag.add(job);
        assertThat(dag.getNoOfJobs(), is(1));
        boolean removed = dag.remove(job);
        assertThat(removed, is(true));
        assertThat(dag.getNoOfJobs(), is(0));
    }

    @Test
    public void testRemoveJobByID() {
        ADag dag = new ADag();
        dag.add(createJob("removeByID"));
        boolean removed = dag.remove("removeByID");
        assertThat(removed, is(true));
        assertThat(dag.getNoOfJobs(), is(0));
    }

    @Test
    public void testRemoveNonExistentJobReturnsFalse() {
        ADag dag = new ADag();
        assertThat(dag.remove("ghost"), is(false));
    }

    // -----------------------------------------------------------------------
    // Edges
    // -----------------------------------------------------------------------

    @Test
    public void testAddEdgeCreatesParentChildRelationship() {
        ADag dag = new ADag();
        dag.add(createJob("parent"));
        dag.add(createJob("child"));
        dag.addNewRelation("parent", "child");

        GraphNode parentNode = dag.getNode("parent");
        assertThat(parentNode.getChildren(), hasSize(1));
        GraphNode firstChild = parentNode.getChildren().iterator().next();
        assertThat(firstChild.getID(), is("child"));
    }

    @Test
    public void testGetRootsForLinearChain() {
        ADag dag = new ADag();
        dag.add(createJob("root"));
        dag.add(createJob("middle"));
        dag.add(createJob("leaf"));
        dag.addNewRelation("root", "middle");
        dag.addNewRelation("middle", "leaf");

        List<GraphNode> roots = dag.getRoots();
        assertThat(roots, hasSize(1));
        assertThat(roots.get(0).getID(), is("root"));
    }

    @Test
    public void testGetLeavesForLinearChain() {
        ADag dag = new ADag();
        dag.add(createJob("root"));
        dag.add(createJob("leaf"));
        dag.addNewRelation("root", "leaf");

        List<GraphNode> leaves = dag.getLeaves();
        assertThat(leaves, hasSize(1));
        assertThat(leaves.get(0).getID(), is("leaf"));
    }

    @Test
    public void testResetEdgesPreservesNodes() {
        ADag dag = new ADag();
        dag.add(createJob("n1"));
        dag.add(createJob("n2"));
        dag.addNewRelation("n1", "n2");
        dag.resetEdges();

        assertThat(dag.getNoOfJobs(), is(2));
        // after reset, n1 should have no children
        assertThat(dag.getNode("n1").getChildren(), hasSize(0));
    }

    // -----------------------------------------------------------------------
    // Iterators
    // -----------------------------------------------------------------------

    @Test
    public void testJobIteratorVisitsAllJobs() {
        ADag dag = new ADag();
        dag.add(createJob("i1"));
        dag.add(createJob("i2"));

        int count = 0;
        for (Iterator<GraphNode> it = dag.jobIterator(); it.hasNext(); ) {
            it.next();
            count++;
        }
        assertThat(count, is(2));
    }

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    @Test
    public void testAddAndGetMetadata() {
        ADag dag = new ADag();
        dag.addMetadata("wf_api", "Python");
        assertThat(dag.getMetadata("wf_api"), is("Python"));
    }

    @Test
    public void testGetMetadataMissingKeyReturnsNull() {
        ADag dag = new ADag();
        assertThat(dag.getMetadata("nonexistent-key"), is(nullValue()));
    }

    @Test
    public void testGetWFAPIDefaultsWhenMetadataIsAbsent() {
        ADag dag = new ADag();

        assertThat(dag.getWFAPI(), is(Metadata.DEFAULT_DAX_API));
    }

    @Test
    public void testGetWFAPIPrefersWorkflowApiMetadataOverDaxApiMetadata() {
        ADag dag = new ADag();
        dag.addMetadata(Metadata.DAX_API_KEY, "OldAPI");
        dag.addMetadata(Metadata.WF_API_KEY, "NewAPI");

        assertThat(dag.getWFAPI(), is("NewAPI"));
    }

    @Test
    public void testGetWFAPIFallsBackToDaxApiMetadata() {
        ADag dag = new ADag();
        dag.addMetadata(Metadata.DAX_API_KEY, "LegacyAPI");

        assertThat(dag.getWFAPI(), is("LegacyAPI"));
    }

    @Test
    public void testResetStoresReplacesExistingStoreInstances() {
        ADag dag = new ADag();
        ReplicaStore replicaStore = dag.getReplicaStore();
        TransformationStore transformationStore = dag.getTransformationStore();
        SiteStore siteStore = dag.getSiteStore();

        dag.resetStores();

        assertThat(dag.getReplicaStore(), is(not(sameInstance(replicaStore))));
        assertThat(dag.getTransformationStore(), is(not(sameInstance(transformationStore))));
        assertThat(dag.getSiteStore(), is(not(sameInstance(siteStore))));
        assertThat(dag.getReplicaStore(), is(notNullValue()));
        assertThat(dag.getTransformationStore(), is(notNullValue()));
        assertThat(dag.getSiteStore(), is(notNullValue()));
    }

    // -----------------------------------------------------------------------
    // Clone
    // -----------------------------------------------------------------------

    @Test
    public void testCloneDoesNotCopyLabel() {
        // ADag.clone() creates a new DagInfo — the label is not propagated
        ADag dag = new ADag();
        dag.setLabel("cloned-wf");
        ADag clone = (ADag) dag.clone();
        // clone starts with a fresh DagInfo, so label is the default (empty string)
        assertThat(clone.getLabel(), not(is("cloned-wf")));
    }

    @Test
    public void testClonePreservesBaseSubmitDirectory() {
        ADag dag = new ADag();
        dag.setBaseSubmitDirectory("/submit/dir");
        ADag clone = (ADag) dag.clone();
        assertThat(clone.getBaseSubmitDirectory(), is("/submit/dir"));
    }

    @Test
    public void testClonePreservesWorkflowUUID() {
        ADag dag = new ADag();
        dag.setWorkflowUUID("uuid-xyz");
        ADag clone = (ADag) dag.clone();
        assertThat(clone.getWorkflowUUID(), is("uuid-xyz"));
    }

    @Test
    public void testCloneIsIndependent() {
        ADag dag = new ADag();
        dag.setLabel("original");
        ADag clone = (ADag) dag.clone();
        clone.setLabel("modified");
        assertThat(dag.getLabel(), is("original"));
    }

    @Test
    public void testClonePreservesRequestIdRootUuidAndRefinementState() {
        ADag dag = new ADag();
        dag.setRequestID("request-1");
        dag.setRootWorkflowUUID("root-1");
        dag.setWorkflowRefinementStarted(true);

        ADag clone = (ADag) dag.clone();

        assertThat(clone.getRequestID(), is("request-1"));
        assertThat(clone.getRootWorkflowUUID(), is("root-1"));
        assertThat(clone.hasWorkflowRefinementStarted(), is(true));
    }

    @Test
    public void testCloneCopiesProfilesIndependently() {
        ADag dag = new ADag();
        dag.addMetadata("key", "value");

        ADag clone = (ADag) dag.clone();
        clone.addMetadata("key", "changed");

        assertThat(dag.getMetadata("key"), is("value"));
        assertThat(clone.getMetadata("key"), is("changed"));
    }

    @Test
    public void testCloneRetainsSameStoreReferences() {
        ADag dag = new ADag();
        ReplicaStore replicaStore = dag.getReplicaStore();
        TransformationStore transformationStore = dag.getTransformationStore();
        SiteStore siteStore = dag.getSiteStore();

        ADag clone = (ADag) dag.clone();

        assertThat(clone.getReplicaStore(), is(sameInstance(replicaStore)));
        assertThat(clone.getTransformationStore(), is(sameInstance(transformationStore)));
        assertThat(clone.getSiteStore(), is(sameInstance(siteStore)));
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsSubmitDirectory() {
        ADag dag = new ADag();
        dag.setBaseSubmitDirectory("/mydir");
        String str = dag.toString();
        assertThat(str, containsString("/mydir"));
    }

    @Test
    public void testToStringContainsWorkflowUUID() {
        ADag dag = new ADag();
        dag.setWorkflowUUID("test-uuid-001");
        String str = dag.toString();
        assertThat(str, containsString("test-uuid-001"));
    }

    @Test
    public void testToStringListsJobIDs() {
        ADag dag = new ADag();
        dag.add(createJob("visible-job"));
        String str = dag.toString();
        assertThat(str, containsString("visible-job"));
    }

    // -----------------------------------------------------------------------
    // hasCycles
    // -----------------------------------------------------------------------

    @Test
    public void testNoCyclesInSimpleDAG() {
        ADag dag = new ADag();
        dag.add(createJob("c1"));
        dag.add(createJob("c2"));
        dag.addNewRelation("c1", "c2");
        assertThat(dag.hasCycles(), is(false));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Job createJob(String name) {
        Job job = new Job();
        job.jobName = name;
        job.logicalName = name;
        job.jobClass = Job.COMPUTE_JOB;
        return job;
    }
}
