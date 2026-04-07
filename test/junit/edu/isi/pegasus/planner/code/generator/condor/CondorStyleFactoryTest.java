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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the CondorStyleFactory class. */
public class CondorStyleFactoryTest {

    private static final class NoOpCondorStyle implements CondorStyle {
        @Override
        public void initialize(PegasusBag bag, CredentialHandlerFactory credentialFactory) {}

        @Override
        public void apply(SiteCatalogEntry site) {}

        @Override
        public void apply(Job job) {}

        @Override
        public void apply(AggregatedJob job) {}
    }

    @Test
    public void testDefaultPackageName() {
        assertThat(
                CondorStyleFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.code.generator.condor.style"));
    }

    @Test
    public void testDefaultPackageNameNotNull() {
        assertThat(CondorStyleFactory.DEFAULT_PACKAGE_NAME, notNullValue());
    }

    @Test
    public void testDefaultPackageNameIsNotEmpty() {
        assertThat(CondorStyleFactory.DEFAULT_PACKAGE_NAME.isEmpty(), is(false));
    }

    @Test
    public void testFactoryClassExists() {
        assertThat(CondorStyleFactory.class, notNullValue());
    }

    @Test
    public void testLoadInstanceForJobWithoutInitializeThrows() {
        CondorStyleFactory factory = new CondorStyleFactory();
        Job job = new Job();
        job.setSiteHandle("local");

        CondorStyleFactoryException e =
                assertThrows(CondorStyleFactoryException.class, () -> factory.loadInstance(job));

        assertThat(e.getMessage(), containsString("initialized first"));
    }

    @Test
    public void testLoadInstanceForSiteWithoutInitializeThrows() {
        CondorStyleFactory factory = new CondorStyleFactory();
        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle("local");

        CondorStyleFactoryException e =
                assertThrows(CondorStyleFactoryException.class, () -> factory.loadInstance(site));

        assertThat(e.getMessage(), containsString("initialized first"));
    }

    @Test
    public void testImplementingClassNameTableContainsExpectedMappings() throws Exception {
        Method method = CondorStyleFactory.class.getDeclaredMethod("implementingClassNameTable");
        method.setAccessible(true);

        Map<String, String> table = (Map<String, String>) method.invoke(null);

        assertThat(table.get(Pegasus.CONDOR_STYLE), is("Condor"));
        assertThat(table.get(Pegasus.GLOBUS_STYLE), is("CondorG"));
        assertThat(table.get(Pegasus.SSH_STYLE), is("SSH"));
        assertThat(table.get(Pegasus.PANDA_STYLE), is("Panda"));
    }

    @Test
    public void testPrivatePutAndGetRoundTripCachedImplementation() throws Exception {
        CondorStyleFactory factory = new CondorStyleFactory();
        NoOpCondorStyle style = new NoOpCondorStyle();

        Method put =
                CondorStyleFactory.class.getDeclaredMethod("put", String.class, CondorStyle.class);
        Method get = CondorStyleFactory.class.getDeclaredMethod("get", String.class);
        put.setAccessible(true);
        get.setAccessible(true);

        put.invoke(factory, Pegasus.CONDOR_STYLE, style);

        assertThat(get.invoke(factory, Pegasus.CONDOR_STYLE), sameInstance(style));
    }

    @Test
    public void testPrivateGetForUnknownStyleThrowsFactoryException() throws Exception {
        CondorStyleFactory factory = new CondorStyleFactory();
        Method get = CondorStyleFactory.class.getDeclaredMethod("get", String.class);
        get.setAccessible(true);

        InvocationTargetException e =
                assertThrows(InvocationTargetException.class, () -> get.invoke(factory, "unknown"));

        assertThat(e.getCause() instanceof CondorStyleFactoryException, is(true));
        assertThat(
                e.getCause().getMessage(), containsString("No class found corresponding to style"));
    }
}
