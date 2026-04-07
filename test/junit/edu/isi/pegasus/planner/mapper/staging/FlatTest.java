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
import edu.isi.pegasus.planner.mapper.StagingMapper;
import java.io.File;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Flat staging mapper class structure. */
public class FlatTest {

    @Test
    public void testFlatImplementsStagingMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                StagingMapper.class.isAssignableFrom(Flat.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                Abstract.class.isAssignableFrom(Flat.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testShortNameConstant() {
        org.hamcrest.MatcherAssert.assertThat(Flat.SHORT_NAME, org.hamcrest.Matchers.is("Flat"));
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
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Flat().description(),
                org.hamcrest.Matchers.is("Flat Directory Staging Mapper"));
    }

    @Test
    public void testInitializeSetsVirtualFlatFileFactory() throws Exception {
        Flat flat = new Flat();

        flat.initialize(new PegasusBag(), new Properties());

        org.hamcrest.MatcherAssert.assertThat(
                ReflectionTestUtils.getField(flat, "mFactory"),
                org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testMapToRelativeDirectoryForFlatLfnReturnsCurrentDirectory() {
        Flat flat = new Flat();
        flat.initialize(new PegasusBag(), new Properties());

        File result =
                flat.mapToRelativeDirectory(new Job(), new SiteCatalogEntry("local"), "f.txt");

        org.hamcrest.MatcherAssert.assertThat(result, org.hamcrest.Matchers.is(new File(".")));
    }

    @Test
    public void testMapToRelativeDirectoryForDeepLfnStillReturnsCurrentDirectory() {
        Flat flat = new Flat();
        flat.initialize(new PegasusBag(), new Properties());

        File result =
                flat.mapToRelativeDirectory(
                        new Job(), new SiteCatalogEntry("local"), "a/b/c/output.dat");

        org.hamcrest.MatcherAssert.assertThat(result, org.hamcrest.Matchers.is(new File(".")));
    }

    @Test
    public void testGetRelativeDirectoryAlwaysReturnsCurrentDirectory() {
        Flat flat = new Flat();

        org.hamcrest.MatcherAssert.assertThat(
                flat.getRelativeDirectory("local", "lfn"), org.hamcrest.Matchers.is(new File(".")));
    }
}
