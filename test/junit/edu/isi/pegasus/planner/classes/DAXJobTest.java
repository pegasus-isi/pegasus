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
public class DAXJobTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorDAXFileIsNull() {
        DAXJob job = new DAXJob();
        assertNull(job.getDAXFile());
    }

    @Test
    public void testDefaultConstructorDirectoryIsNull() {
        DAXJob job = new DAXJob();
        assertNull(job.getDirectory());
    }

    @Test
    public void testDefaultConstructorDAXLFNIsNull() {
        DAXJob job = new DAXJob();
        assertNull(job.getDAXLFN());
    }

    @Test
    public void testDefaultConstructorJobTypeIsDAXJob() {
        DAXJob job = new DAXJob();
        assertThat(job.getJobType(), is(Job.DAX_JOB));
    }

    @Test
    public void testSetAndGetDAXLFN() {
        DAXJob job = new DAXJob();
        job.setDAXLFN("workflow.dax");
        assertThat(job.getDAXLFN(), is("workflow.dax"));
    }

    @Test
    public void testSetAndGetDAXFile() {
        DAXJob job = new DAXJob();
        job.setDAXFile("/path/to/workflow.dax");
        assertThat(job.getDAXFile(), is("/path/to/workflow.dax"));
    }

    @Test
    public void testSetAndGetDirectory() {
        DAXJob job = new DAXJob();
        job.setDirectory("/scratch/run");
        assertThat(job.getDirectory(), is("/scratch/run"));
    }

    @Test
    public void testSetDAXLFNOverwritesPreviousValue() {
        DAXJob job = new DAXJob();
        job.setDAXLFN("first.dax");
        job.setDAXLFN("second.dax");
        assertThat(job.getDAXLFN(), is("second.dax"));
    }

    @Test
    public void testSetDirectoryOverwritesPreviousValue() {
        DAXJob job = new DAXJob();
        job.setDirectory("/dir1");
        job.setDirectory("/dir2");
        assertThat(job.getDirectory(), is("/dir2"));
    }

    @Test
    public void testConstructorFromJobSetsTypeToDAXJob() {
        Job baseJob = new Job();
        DAXJob job = new DAXJob(baseJob);
        assertThat(job.getJobType(), is(Job.DAX_JOB));
    }

    @Test
    public void testConstructorFromJobDAXFileIsNull() {
        Job baseJob = new Job();
        DAXJob job = new DAXJob(baseJob);
        assertNull(job.getDAXFile());
    }

    @Test
    public void testClonePreservesDAXLFN() {
        DAXJob original = new DAXJob();
        original.setDAXLFN("sub.dax");
        DAXJob clone = (DAXJob) original.clone();
        assertThat(clone.getDAXLFN(), is("sub.dax"));
    }

    @Test
    public void testClonePreservesDAXFile() {
        DAXJob original = new DAXJob();
        original.setDAXFile("/tmp/sub.dax");
        DAXJob clone = (DAXJob) original.clone();
        assertThat(clone.getDAXFile(), is("/tmp/sub.dax"));
    }

    @Test
    public void testClonePreservesDirectory() {
        DAXJob original = new DAXJob();
        original.setDirectory("/work/dir");
        DAXJob clone = (DAXJob) original.clone();
        assertThat(clone.getDirectory(), is("/work/dir"));
    }

    @Test
    public void testCloneIsIndependentObject() {
        DAXJob original = new DAXJob();
        original.setDAXLFN("sub.dax");
        DAXJob clone = (DAXJob) original.clone();
        assertNotSame(original, clone);
    }

    @Test
    public void testJobPrefixConstant() {
        assertThat(DAXJob.JOB_PREFIX, is("subdax_"));
    }

    @Test
    public void testGenerateNameWithLFNNoExtension() {
        DAXJob job = new DAXJob();
        job.setDAXLFN("mywf");
        job.setLogicalID("0001");
        String name = job.generateName(null);
        assertThat(name, is("subdax_mywf_0001"));
    }

    @Test
    public void testGenerateNameStripsExtensionFromLFN() {
        DAXJob job = new DAXJob();
        job.setDAXLFN("mywf.dax");
        job.setLogicalID("0001");
        String name = job.generateName(null);
        assertThat(name, is("subdax_mywf_0001"));
    }

    @Test
    public void testGenerateNameWithPrefix() {
        DAXJob job = new DAXJob();
        job.setDAXLFN("wf.dax");
        job.setLogicalID("42");
        String name = job.generateName("run0_");
        assertThat(name, is("run0_subdax_wf_42"));
    }

    @Test
    public void testGenerateNameThrowsWhenLFNNotSet() {
        DAXJob job = new DAXJob();
        job.setLogicalID("1");
        assertThrows(RuntimeException.class, () -> job.generateName(null));
    }

    @Test
    public void testSetAndGetInputWorkflowCacheFile() {
        DAXJob job = new DAXJob();
        job.setInputWorkflowCacheFile("/tmp/cache.txt");
        assertThat(job.getInputWorkflowCacheFile(), is("/tmp/cache.txt"));
    }
}
