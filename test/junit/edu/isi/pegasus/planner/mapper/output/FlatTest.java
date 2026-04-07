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
package edu.isi.pegasus.planner.mapper.output;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import org.griphyn.vdl.euryale.VirtualFlatFileFactory;
import org.junit.jupiter.api.Test;

/** Tests for the Flat output mapper class structure. */
public class FlatTest {

    @Test
    public void testFlatImplementsOutputMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                OutputMapper.class.isAssignableFrom(Flat.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatExtendsAbstractFileFactoryBasedMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Flat.class),
                org.hamcrest.Matchers.is(true));
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
                new Flat().description(), org.hamcrest.Matchers.is("Flat Directory Mapper"));
    }

    @Test
    public void testGetShortNameReturnsConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                new Flat().getShortName(), org.hamcrest.Matchers.is(Flat.SHORT_NAME));
    }

    @Test
    public void testInstantiateFileFactoryReturnsVirtualFlatFileFactory() {
        Flat flat = new Flat();
        flat.mSiteStore = new SiteStore();

        org.hamcrest.MatcherAssert.assertThat(
                flat.instantiateFileFactory(new PegasusBag(), new ADag())
                        instanceof VirtualFlatFileFactory,
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testCreateAndGetAddOnReturnsFactoryPathForLfn() {
        Flat flat = new Flat();
        flat.mSiteStore = new SiteStore();
        flat.mFactory = flat.instantiateFileFactory(new PegasusBag(), new ADag());

        String addOn = flat.createAndGetAddOn("f.out", "local", false);

        org.hamcrest.MatcherAssert.assertThat(addOn, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(
                addOn.endsWith("f.out"), org.hamcrest.Matchers.is(true));
    }
}
