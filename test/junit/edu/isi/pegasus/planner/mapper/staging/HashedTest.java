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
package edu.isi.pegasus.planner.mapper.staging;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Hashed staging mapper class structure. */
public class HashedTest {

    @Test
    public void testHashedImplementsStagingMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                StagingMapper.class.isAssignableFrom(Hashed.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testHashedExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                Abstract.class.isAssignableFrom(Hashed.class), org.hamcrest.Matchers.is(true));
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
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Hashed().description(),
                org.hamcrest.Matchers.is("Hashed Directory Staging Mapper"));
    }

    @Test
    public void testInitializeSetsFactoryAndTrackingMap() throws Exception {
        Hashed hashed = new Hashed();
        Properties properties = new Properties();
        properties.setProperty(Hashed.MULIPLICATOR_PROPERTY_KEY, "7");
        properties.setProperty(Hashed.LEVELS_PROPERTY_KEY, "3");

        hashed.initialize(new PegasusBag(), properties);

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(hashed, "mFactory"),
                org.hamcrest.Matchers.notNullValue());

        org.hamcrest.MatcherAssert.assertThat(
                ((Map<?, ?>) ReflectionTestUtils.getField(hashed, "mSiteLFNAddOnMap")).isEmpty(),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testMapToRelativeDirectoryReusesAddonWithinSameJob() {
        Hashed hashed = new Hashed();
        hashed.initialize(new PegasusBag(), new Properties());
        SiteCatalogEntry site = new SiteCatalogEntry("local");
        Job job = new Job();
        job.setName("jobA");

        File first = hashed.mapToRelativeDirectory(job, site, "a.dat");
        File second = hashed.mapToRelativeDirectory(job, site, "nested/b.dat");

        org.hamcrest.MatcherAssert.assertThat(second, org.hamcrest.Matchers.is(first));
    }

    @Test
    public void testMapToRelativeDirectoryTracksAddonPerSiteAndLfn() {
        Hashed hashed = new Hashed();
        hashed.initialize(new PegasusBag(), new Properties());
        SiteCatalogEntry site = new SiteCatalogEntry("local");

        Job firstJob = new Job();
        firstJob.setName("jobA");
        File first = hashed.mapToRelativeDirectory(firstJob, site, "input.txt");

        Job secondJob = new Job();
        secondJob.setName("jobB");
        File second = hashed.mapToRelativeDirectory(secondJob, site, "input.txt");

        org.hamcrest.MatcherAssert.assertThat(second, org.hamcrest.Matchers.is(first));
        org.hamcrest.MatcherAssert.assertThat(
                hashed.getRelativeDirectory("local", "input.txt"), org.hamcrest.Matchers.is(first));
    }

    @Test
    public void testGetRelativeDirectoryThrowsForUnknownMapping() {
        Hashed hashed = new Hashed();
        hashed.initialize(new PegasusBag(), new Properties());

        MapperException e =
                assertThrows(
                        MapperException.class,
                        () -> hashed.getRelativeDirectory("local", "missing.txt"));

        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage()
                        .contains(
                                "Hashed Directory Staging Mapper unable to retrieve relative directory"),
                org.hamcrest.Matchers.is(true));
    }
}
