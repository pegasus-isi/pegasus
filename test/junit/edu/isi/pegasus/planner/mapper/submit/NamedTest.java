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
package edu.isi.pegasus.planner.mapper.submit;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Named submit mapper class structure. */
public class NamedTest {

    @Test
    public void testNamedImplementsSubmitMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                SubmitMapper.class.isAssignableFrom(Named.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testDefaultInstantiation() {
        Named named = new Named();
        org.hamcrest.MatcherAssert.assertThat(named, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testNamedIsPublicClass() {
        int modifiers = Named.class.getModifiers();
        org.hamcrest.MatcherAssert.assertThat(
                java.lang.reflect.Modifier.isPublic(modifiers), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testGetRelativeDirMethodExists() throws NoSuchMethodException {
        org.hamcrest.MatcherAssert.assertThat(
                Named.class.getMethod("getRelativeDir", edu.isi.pegasus.planner.classes.Job.class),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testGetDirMethodExists() throws NoSuchMethodException {
        org.hamcrest.MatcherAssert.assertThat(
                Named.class.getMethod("getDir", edu.isi.pegasus.planner.classes.Job.class),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Named().description(),
                org.hamcrest.Matchers.is("Relative Submit Directory Mapper"));
    }

    @Test
    public void testInitializeStoresBaseDirectories() throws Exception {
        Named named = new Named();
        File base = new File("/tmp/submit-base");
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(base.getPath(), "wf");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);

        named.initialize(bag, new Properties(), base);

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(named, "mBaseDir"), org.hamcrest.Matchers.is(base));
        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(named, "mBaseSubmitDirectory"),
                org.hamcrest.Matchers.is(new File(base, "wf")));
    }

    @Test
    public void testDetermineRelativeDirectoryForAuxiliaryJobUsesCurrentDirectory() {
        TestNamed named = new TestNamed();
        Job job = new Job();
        job.setJobType(Job.CLEANUP_JOB);

        org.hamcrest.MatcherAssert.assertThat(named.determine(job), org.hamcrest.Matchers.is("."));
    }

    @Test
    public void testDetermineRelativeDirectoryForComputeJobUsesPegasusProfile() {
        TestNamed named = new TestNamed();
        Job job = new Job();
        job.setName("compute");
        job.setJobType(Job.COMPUTE_JOB);
        job.vdsNS.construct(Pegasus.RELATIVE_SUBMIT_DIR_KEY, "custom-dir");

        org.hamcrest.MatcherAssert.assertThat(
                named.determine(job), org.hamcrest.Matchers.is("custom-dir"));
    }

    @Test
    public void testDetermineRelativeDirectoryForComputeJobFallsBackToTransformation() {
        TestNamed named = new TestNamed();
        Job job = new Job();
        job.setName("compute");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTXName("transform");

        org.hamcrest.MatcherAssert.assertThat(
                named.determine(job), org.hamcrest.Matchers.is("transform"));
    }

    @Test
    public void testDetermineRelativeDirectoryForComputeJobWithoutProfileOrTransformationThrows() {
        TestNamed named = new TestNamed();
        Job job = new Job();
        job.setName("compute");
        job.setJobType(Job.COMPUTE_JOB);

        MapperException e = assertThrows(MapperException.class, () -> named.determine(job));
        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage()
                        .contains(
                                "Pegasus Profile Key relative.submit.dir not specified. Unable to determine relative directory"),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testGetRelativeDirCreatesDirectoryUnderSubmitBase() throws Exception {
        Path temp = Files.createTempDirectory("named-submit-test");
        File base = temp.toFile();
        Named named = initializedNamed(base, "wf");
        Job job = new Job();
        job.setName("compute");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTXName("transform");

        File relative = named.getRelativeDir(job);

        org.hamcrest.MatcherAssert.assertThat(
                relative, org.hamcrest.Matchers.is(new File("transform")));
        org.hamcrest.MatcherAssert.assertThat(
                new File(new File(base, "wf"), "transform").isDirectory(),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testGetDirReturnsFullPathForComputeJob() {
        File base = new File("/tmp/submit-base");
        Named named = initializedNamed(base, "wf");
        Job job = new Job();
        job.setName("compute");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTXName("transform");

        org.hamcrest.MatcherAssert.assertThat(
                named.getDir(job),
                org.hamcrest.Matchers.is(new File(new File(base, "wf"), "transform")));
    }

    private Named initializedNamed(File base, String relative) {
        Named named = new Named();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(base.getPath(), relative);
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        named.initialize(bag, new Properties(), base);
        return named;
    }

    private static final class TestNamed extends Named {
        String determine(Job job) {
            return this.determineRelativeDirectory(job);
        }
    }
}
