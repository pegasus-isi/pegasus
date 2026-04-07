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
import java.lang.reflect.Method;
import java.util.Collections;
import org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;
import org.junit.jupiter.api.Test;

/** Tests for the Hashed output mapper class structure. */
public class HashedTest {

    @Test
    public void testHashedImplementsOutputMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                OutputMapper.class.isAssignableFrom(Hashed.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testHashedExtendsAbstractFileFactoryBasedMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Hashed.class),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testShortNameConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Hashed.SHORT_NAME, org.hamcrest.Matchers.is("Hashed"));
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
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Hashed().description(), org.hamcrest.Matchers.is("Hashed Directory Mapper"));
    }

    @Test
    public void testGetShortNameReturnsConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                new Hashed().getShortName(), org.hamcrest.Matchers.is(Hashed.SHORT_NAME));
    }

    @Test
    public void testInstantiateFileFactoryReturnsVirtualDecimalHashedFileFactory() {
        Hashed hashed = new Hashed();
        hashed.mSiteStore = new SiteStore();

        org.hamcrest.MatcherAssert.assertThat(
                hashed.instantiateFileFactory(new PegasusBag(), new ADag())
                        instanceof VirtualDecimalHashedFileFactory,
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testCreateAndGetAddOnTracksExistingLfnForSite() throws Exception {
        Hashed hashed = new Hashed();
        hashed.mSiteStore = new SiteStore();
        hashed.mFactory = hashed.instantiateFileFactory(new PegasusBag(), new ADag());
        hashed.mOutputSites = Collections.singleton("local");
        invokeResetLFNAddOnCache(hashed);

        String created = hashed.createAndGetAddOn("f.out", "local", false);
        String existing = hashed.createAndGetAddOn("f.out", "local", true);

        org.hamcrest.MatcherAssert.assertThat(created, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(existing, org.hamcrest.Matchers.is(created));
        org.hamcrest.MatcherAssert.assertThat(
                created.endsWith("f.out"), org.hamcrest.Matchers.is(true));
    }

    private static void invokeResetLFNAddOnCache(Hashed hashed) throws Exception {
        Method method = Hashed.class.getDeclaredMethod("resetLFNAddOnCache");
        method.setAccessible(true);
        method.invoke(hashed);
    }
}
