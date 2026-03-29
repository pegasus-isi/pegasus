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
public class DAGJobTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorDAGFileIsNull() {
        DAGJob job = new DAGJob();
        assertNull(job.getDAGFile());
    }

    @Test
    public void testDefaultConstructorDirectoryIsNull() {
        DAGJob job = new DAGJob();
        assertNull(job.getDirectory());
    }

    @Test
    public void testDefaultConstructorDAGLFNIsNull() {
        DAGJob job = new DAGJob();
        assertNull(job.getDAGLFN());
    }

    @Test
    public void testDefaultConstructorJobTypeIsDAGJob() {
        DAGJob job = new DAGJob();
        assertThat(job.getJobType(), is(Job.DAG_JOB));
    }

    @Test
    public void testSetAndGetDAGLFN() {
        DAGJob job = new DAGJob();
        job.setDAGLFN("workflow.dag");
        assertThat(job.getDAGLFN(), is("workflow.dag"));
    }

    @Test
    public void testSetAndGetDAGFile() {
        DAGJob job = new DAGJob();
        job.setDAGFile("/path/to/workflow.dag");
        assertThat(job.getDAGFile(), is("/path/to/workflow.dag"));
    }

    @Test
    public void testSetAndGetDirectory() {
        DAGJob job = new DAGJob();
        job.setDirectory("/scratch/run");
        assertThat(job.getDirectory(), is("/scratch/run"));
    }

    @Test
    public void testSetDAGLFNOverwritesPreviousValue() {
        DAGJob job = new DAGJob();
        job.setDAGLFN("first.dag");
        job.setDAGLFN("second.dag");
        assertThat(job.getDAGLFN(), is("second.dag"));
    }

    @Test
    public void testSetDirectoryOverwritesPreviousValue() {
        DAGJob job = new DAGJob();
        job.setDirectory("/dir1");
        job.setDirectory("/dir2");
        assertThat(job.getDirectory(), is("/dir2"));
    }

    @Test
    public void testConstructorFromJobSetsTypeToDAGJob() {
        Job baseJob = new Job();
        DAGJob job = new DAGJob(baseJob);
        assertThat(job.getJobType(), is(Job.DAG_JOB));
    }

    @Test
    public void testConstructorFromJobDAGFileIsNull() {
        Job baseJob = new Job();
        DAGJob job = new DAGJob(baseJob);
        assertNull(job.getDAGFile());
    }

    @Test
    public void testClonePreservesDAGLFN() {
        DAGJob original = new DAGJob();
        original.setDAGLFN("sub.dag");
        DAGJob clone = (DAGJob) original.clone();
        assertThat(clone.getDAGLFN(), is("sub.dag"));
    }

    @Test
    public void testClonePreservesDAGFile() {
        DAGJob original = new DAGJob();
        original.setDAGFile("/tmp/sub.dag");
        DAGJob clone = (DAGJob) original.clone();
        assertThat(clone.getDAGFile(), is("/tmp/sub.dag"));
    }

    @Test
    public void testClonePreservesDirectory() {
        DAGJob original = new DAGJob();
        original.setDirectory("/work/dir");
        DAGJob clone = (DAGJob) original.clone();
        assertThat(clone.getDirectory(), is("/work/dir"));
    }

    @Test
    public void testCloneIsIndependentObject() {
        DAGJob original = new DAGJob();
        original.setDAGLFN("sub.dag");
        DAGJob clone = (DAGJob) original.clone();
        assertNotSame(original, clone);
    }

    @Test
    public void testJobPrefixConstant() {
        assertThat(DAGJob.JOB_PREFIX, is("subdag_"));
    }

    @Test
    public void testGenerateNameWithLFNNoExtension() {
        DAGJob job = new DAGJob();
        job.setDAGLFN("mywf");
        job.setLogicalID("0001");
        String name = job.generateName(null);
        assertThat(name, is("subdag_mywf_0001"));
    }

    @Test
    public void testGenerateNameStripsExtensionFromLFN() {
        DAGJob job = new DAGJob();
        job.setDAGLFN("mywf.dag");
        job.setLogicalID("0001");
        String name = job.generateName(null);
        assertThat(name, is("subdag_mywf_0001"));
    }

    @Test
    public void testGenerateNameWithPrefix() {
        DAGJob job = new DAGJob();
        job.setDAGLFN("wf.dag");
        job.setLogicalID("42");
        String name = job.generateName("run0_");
        assertThat(name, is("run0_subdag_wf_42"));
    }

    @Test
    public void testGenerateNameThrowsWhenLFNNotSet() {
        DAGJob job = new DAGJob();
        job.setLogicalID("1");
        assertThrows(RuntimeException.class, () -> job.generateName(null));
    }
}
