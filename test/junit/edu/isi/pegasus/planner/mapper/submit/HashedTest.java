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

/** Tests for the Hashed submit mapper class structure. */
public class HashedTest {

    @Test
    public void testHashedImplementsSubmitMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                SubmitMapper.class.isAssignableFrom(Hashed.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testMultiplicatorPropertyKeyConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Hashed.MULIPLICATOR_PROPERTY_KEY, org.hamcrest.Matchers.is("hashed.multiplier"));
    }

    @Test
    public void testDefaultMultiplicatorFactorConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Hashed.DEFAULT_MULTIPLICATOR_FACTOR, org.hamcrest.Matchers.is(5));
    }

    @Test
    public void testLevelsPropertyKeyConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Hashed.LEVELS_PROPERTY_KEY, org.hamcrest.Matchers.is("hashed.levels"));
    }

    @Test
    public void testDefaultLevelsConstant() {
        org.hamcrest.MatcherAssert.assertThat(Hashed.DEFAULT_LEVELS, org.hamcrest.Matchers.is(2));
    }

    @Test
    public void testDefaultInstantiation() {
        Hashed hashed = new Hashed();
        org.hamcrest.MatcherAssert.assertThat(hashed, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testHashedIsPublicClass() {
        int modifiers = Hashed.class.getModifiers();
        org.hamcrest.MatcherAssert.assertThat(
                java.lang.reflect.Modifier.isPublic(modifiers), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testInitializeSetsBaseDirectoryAndFactory() throws Exception {
        Hashed hashed = new Hashed();
        Properties properties = new Properties();
        properties.setProperty(Hashed.MULIPLICATOR_PROPERTY_KEY, "7");
        properties.setProperty(Hashed.LEVELS_PROPERTY_KEY, "3");
        File base = new File("/tmp/submit-base");

        hashed.initialize(bagFor(base, "wf"), properties, base);

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(hashed, "mBaseDir"), org.hamcrest.Matchers.is(base));

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(hashed, "mFactory"),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testDescriptionIncludesDefaultFactorySettingsAfterInitialize() {
        Hashed hashed = new Hashed();
        File base = new File("/tmp/submit-base");

        hashed.initialize(bagFor(base, "wf"), new Properties(), base);

        org.hamcrest.MatcherAssert.assertThat(
                hashed.description(),
                org.hamcrest.Matchers.is(
                        "Hashed Directory Mapper with multiplier as 5 and levels 2"));
    }

    @Test
    public void testDescriptionIncludesConfiguredFactorySettings() {
        Hashed hashed = new Hashed();
        Properties properties = new Properties();
        properties.setProperty(Hashed.MULIPLICATOR_PROPERTY_KEY, "7");
        properties.setProperty(Hashed.LEVELS_PROPERTY_KEY, "3");
        File base = new File("/tmp/submit-base");

        hashed.initialize(bagFor(base, "wf"), properties, base);

        org.hamcrest.MatcherAssert.assertThat(
                hashed.description(),
                org.hamcrest.Matchers.is(
                        "Hashed Directory Mapper with multiplier as 7 and levels 3"));
    }

    @Test
    public void testGetRelativeDirReturnsRelativePath() {
        Hashed hashed = new Hashed();
        File base = new File("/tmp/submit-base");
        hashed.initialize(bagFor(base, "wf"), new Properties(), base);

        Job job = new Job();
        job.setName("jobA");

        File relative = hashed.getRelativeDir(job);
        org.hamcrest.MatcherAssert.assertThat(relative, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(
                relative.isAbsolute(), org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testGetDirReturnsPathUnderSubmitDirectory() {
        Hashed hashed = new Hashed();
        File base = new File("/tmp/submit-base");
        hashed.initialize(bagFor(base, "wf"), new Properties(), base);

        Job job = new Job();
        job.setName("jobA");

        File dir = hashed.getDir(job);
        org.hamcrest.MatcherAssert.assertThat(dir, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(
                dir.getPath().startsWith(new File(base, "wf").getPath()),
                org.hamcrest.Matchers.is(true));
    }

    private PegasusBag bagFor(File base, String relative) {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(base.getPath(), relative);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        return bag;
    }
}
