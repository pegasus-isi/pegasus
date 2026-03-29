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
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WorkflowKeywordsTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testPegasusReservedName() {
        assertThat(WorkflowKeywords.PEGASUS.getReservedName(), is("pegasus"));
    }

    @Test
    public void testXPegasusReservedName() {
        assertThat(WorkflowKeywords.X_PEGASUS.getReservedName(), is("x-pegasus"));
    }

    @Test
    public void testNameReservedName() {
        assertThat(WorkflowKeywords.NAME.getReservedName(), is("name"));
    }

    @Test
    public void testWorkflowReservedName() {
        assertThat(WorkflowKeywords.WORKFLOW.getReservedName(), is("workflow"));
    }

    @Test
    public void testJobsReservedName() {
        assertThat(WorkflowKeywords.JOBS.getReservedName(), is("jobs"));
    }

    @Test
    public void testJobIdReservedName() {
        assertThat(WorkflowKeywords.JOB_ID.getReservedName(), is("id"));
    }

    @Test
    public void testJobArgumentsReservedName() {
        assertThat(WorkflowKeywords.JOB_ARGUMENTS.getReservedName(), is("arguments"));
    }

    @Test
    public void testJobDependenciesReservedName() {
        assertThat(WorkflowKeywords.JOB_DEPENDENCIES.getReservedName(), is("jobDependencies"));
    }

    @Test
    public void testChildrenReservedName() {
        assertThat(WorkflowKeywords.CHILDREN.getReservedName(), is("children"));
    }

    @Test
    public void testUsesReservedName() {
        assertThat(WorkflowKeywords.USES.getReservedName(), is("uses"));
    }

    @Test
    public void testLfnReservedName() {
        assertThat(WorkflowKeywords.LFN.getReservedName(), is("lfn"));
    }

    @Test
    public void testProfilesReservedName() {
        assertThat(WorkflowKeywords.PROFILES.getReservedName(), is("profiles"));
    }

    @Test
    public void testMetadataReservedName() {
        assertThat(WorkflowKeywords.METADATA.getReservedName(), is("metadata"));
    }

    @Test
    public void testGetReservedKeyReturnsCorrectEnum() {
        assertThat(WorkflowKeywords.getReservedKey("pegasus"), is(WorkflowKeywords.PEGASUS));
    }

    @Test
    public void testGetReservedKeyForJobs() {
        assertThat(WorkflowKeywords.getReservedKey("jobs"), is(WorkflowKeywords.JOBS));
    }

    @Test
    public void testGetReservedKeyForJobDependencies() {
        assertThat(
                WorkflowKeywords.getReservedKey("jobDependencies"),
                is(WorkflowKeywords.JOB_DEPENDENCIES));
    }

    @Test
    public void testGetReservedKeyForChildren() {
        assertThat(WorkflowKeywords.getReservedKey("children"), is(WorkflowKeywords.CHILDREN));
    }

    @Test
    public void testGetReservedKeyForUnknownReturnsNull() {
        assertNull(WorkflowKeywords.getReservedKey("unknownKeyword"));
    }

    @Test
    public void testGetReservedKeyIsCaseSensitive() {
        // reserved names are lowercase; uppercase lookup should return null
        assertNull(WorkflowKeywords.getReservedKey("Pegasus"));
    }

    @Test
    public void testAllEnumValuesHaveNonNullReservedName() {
        for (WorkflowKeywords kw : WorkflowKeywords.values()) {
            assertNotNull(
                    kw.getReservedName(), kw.name() + " should have a non-null reserved name");
        }
    }

    @Test
    public void testReplicaCatalogReservedName() {
        assertThat(WorkflowKeywords.REPLICA_CATALOG.getReservedName(), is("replicaCatalog"));
    }

    @Test
    public void testSiteCatalogReservedName() {
        assertThat(WorkflowKeywords.SITE_CATALOG.getReservedName(), is("siteCatalog"));
    }

    @Test
    public void testTransformationCatalogReservedName() {
        assertThat(
                WorkflowKeywords.TRANSFORMATION_CATALOG.getReservedName(),
                is("transformationCatalog"));
    }

    @Test
    public void testStageOutReservedName() {
        assertThat(WorkflowKeywords.STAGE_OUT.getReservedName(), is("stageOut"));
    }

    @Test
    public void testRegisterReplicaReservedName() {
        assertThat(WorkflowKeywords.REGISTER_REPLICA.getReservedName(), is("registerReplica"));
    }
}
