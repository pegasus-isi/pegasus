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
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for the CondorStyle interface. */
public class CondorStyleTest {

    @Test
    public void testVersionConstant() {
        assertThat(CondorStyle.VERSION, is("1.4"));
    }

    @Test
    public void testVersionConstantNotNull() {
        assertThat(CondorStyle.VERSION, notNullValue());
    }

    @Test
    public void testCondorStyleClassExists() {
        assertThat(CondorStyle.class, notNullValue());
    }

    @Test
    public void testCondorStyleImplementedByAbstractStyle() {
        assertThat(
                CondorStyle.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.generator.condor.style.Abstract.class),
                is(true));
    }

    @Test
    public void testCondorStyleIsInterface() {
        assertThat(CondorStyle.class.isInterface(), is(true));
    }

    @Test
    public void testInitializeMethodSignature() throws Exception {
        Method method =
                CondorStyle.class.getMethod(
                        "initialize", PegasusBag.class, CredentialHandlerFactory.class);

        assertThat(method.getExceptionTypes(), arrayContaining(CondorStyleException.class));
    }

    @Test
    public void testApplyMethodForSiteCatalogEntrySignature() throws Exception {
        Method method = CondorStyle.class.getMethod("apply", SiteCatalogEntry.class);

        assertThat(method.getExceptionTypes(), arrayContaining(CondorStyleException.class));
    }

    @Test
    public void testApplyMethodForJobSignature() throws Exception {
        Method method = CondorStyle.class.getMethod("apply", Job.class);

        assertThat(method.getExceptionTypes(), arrayContaining(CondorStyleException.class));
    }

    @Test
    public void testApplyMethodForAggregatedJobSignature() throws Exception {
        Method method = CondorStyle.class.getMethod("apply", AggregatedJob.class);

        assertThat(method.getExceptionTypes(), arrayContaining(CondorStyleException.class));
    }
}
