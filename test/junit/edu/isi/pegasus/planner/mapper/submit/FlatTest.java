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
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import java.io.File;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Flat submit mapper class structure. */
public class FlatTest {

    @Test
    public void testFlatImplementsSubmitMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                SubmitMapper.class.isAssignableFrom(Flat.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testDefaultInstantiation() {
        Flat flat = new Flat();
        org.hamcrest.MatcherAssert.assertThat(flat, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testFlatIsPublicClass() {
        int modifiers = Flat.class.getModifiers();
        org.hamcrest.MatcherAssert.assertThat(
                java.lang.reflect.Modifier.isPublic(modifiers), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testGetRelativeDirMethodExists() throws NoSuchMethodException {
        org.hamcrest.MatcherAssert.assertThat(
                Flat.class.getMethod("getRelativeDir", edu.isi.pegasus.planner.classes.Job.class),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testGetDirMethodExists() throws NoSuchMethodException {
        org.hamcrest.MatcherAssert.assertThat(
                Flat.class.getMethod("getDir", edu.isi.pegasus.planner.classes.Job.class),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Flat().description(), org.hamcrest.Matchers.is("Flat Submit Directory Mapper"));
    }

    @Test
    public void testInitializeSetsBaseDirectoryAndFactory() throws Exception {
        Flat flat = new Flat();
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory("/tmp/submit-base", "wf");
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        File base = new File("/tmp/submit-base");

        flat.initialize(bag, new Properties(), base);

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(flat, "mBaseDir"), org.hamcrest.Matchers.is(base));

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(flat, "mFactory"),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testGetRelativeDirReturnsCurrentDirectory() {
        Flat flat = initializedFlat("/tmp/submit-base", "wf");

        Job job = new Job();
        job.setName("jobA");

        org.hamcrest.MatcherAssert.assertThat(
                flat.getRelativeDir(job), org.hamcrest.Matchers.is(new File(".")));
    }

    @Test
    public void testGetDirReturnsSubmitDirectory() {
        Flat flat = initializedFlat("/tmp/submit-base", "wf");

        Job job = new Job();
        job.setName("jobA");

        org.hamcrest.MatcherAssert.assertThat(
                flat.getDir(job), org.hamcrest.Matchers.is(new File("/tmp/submit-base/wf")));
    }

    private Flat initializedFlat(String base, String relative) {
        Flat flat = new Flat();
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(base, relative);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        flat.initialize(bag, new Properties(), new File(base));
        return flat;
    }
}
