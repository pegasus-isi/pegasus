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
package edu.isi.pegasus.planner.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SLSTest {

    @Test
    public void testSLSIsInterfaceAndVersionConstant() {
        assertThat(SLS.class.isInterface(), is(true));
        assertThat(SLS.VERSION, equalTo("1.4"));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                SLS.class.getDeclaredMethod("initialize", PegasusBag.class).getReturnType(),
                equalTo(void.class));
        assertThat(
                SLS.class.getDeclaredMethod("doesCondorModifications").getReturnType(),
                equalTo(boolean.class));
        assertThat(
                SLS.class
                        .getDeclaredMethod("invocationString", Job.class, File.class)
                        .getReturnType(),
                equalTo(String.class));
        assertThat(
                SLS.class.getDeclaredMethod("needsSLSInputTransfers", Job.class).getReturnType(),
                equalTo(boolean.class));
        assertThat(
                SLS.class.getDeclaredMethod("needsSLSOutputTransfers", Job.class).getReturnType(),
                equalTo(boolean.class));
        assertThat(
                SLS.class.getDeclaredMethod("getSLSInputLFN", Job.class).getReturnType(),
                equalTo(String.class));
        assertThat(
                SLS.class.getDeclaredMethod("getSLSOutputLFN", Job.class).getReturnType(),
                equalTo(String.class));
        assertThat(
                SLS.class
                        .getDeclaredMethod(
                                "determineSLSInputTransfers",
                                Job.class,
                                String.class,
                                FileServer.class,
                                String.class,
                                String.class,
                                boolean.class)
                        .getReturnType(),
                equalTo(Collection.class));
        assertThat(
                SLS.class
                        .getDeclaredMethod(
                                "determineSLSOutputTransfers",
                                Job.class,
                                String.class,
                                FileServer.class,
                                String.class,
                                String.class)
                        .getReturnType(),
                equalTo(Collection.class));
        assertThat(
                SLS.class
                        .getDeclaredMethod(
                                "modifyJobForWorkerNodeExecution",
                                Job.class,
                                String.class,
                                String.class,
                                String.class)
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                SLS.class.getDeclaredMethod("getDescription").getReturnType(),
                equalTo(String.class));
    }

    @Test
    public void testSLSDeclaresExpectedMethods() {
        Method[] methods = SLS.class.getDeclaredMethods();
        assertThat(methods.length, equalTo(11));
        for (Method method : methods) {
            assertThat(Modifier.isPublic(method.getModifiers()), is(true));
            assertThat(Modifier.isAbstract(method.getModifiers()), is(true));
        }
    }
}
