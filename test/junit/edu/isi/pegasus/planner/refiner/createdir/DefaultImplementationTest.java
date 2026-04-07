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
package edu.isi.pegasus.planner.refiner.createdir;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for DefaultImplementation createdir class. */
public class DefaultImplementationTest {

    @Test
    public void testImplementsImplementation() {
        assertThat(Implementation.class.isAssignableFrom(DefaultImplementation.class), is(true));
    }

    @Test
    public void testTransformationNamespace() {
        assertThat(DefaultImplementation.TRANSFORMATION_NAMESPACE, is("pegasus"));
    }

    @Test
    public void testTransformationName() {
        assertThat(DefaultImplementation.TRANSFORMATION_NAME, is("dirmanager"));
    }

    @Test
    public void testExecutableBasename() {
        assertThat(DefaultImplementation.EXECUTABLE_BASENAME, is("pegasus-transfer"));
    }

    @Test
    public void testDefaultConstructor() {
        DefaultImplementation di = new DefaultImplementation();
        assertThat(di, notNullValue());
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(DefaultImplementation.TRANSFORMATION_VERSION, nullValue());
        assertThat(DefaultImplementation.COMPLETE_TRANSFORMATION_NAME, is("pegasus::dirmanager"));
        assertThat(DefaultImplementation.DERIVATION_NAMESPACE, is("pegasus"));
        assertThat(DefaultImplementation.DERIVATION_NAME, is("dirmanager"));
        assertThat(DefaultImplementation.DERIVATION_VERSION, is("1.0"));
        assertThat(DefaultImplementation.PATH_VALUE, is(".:/bin:/usr/bin:/usr/ucb/bin"));
    }

    @Test
    public void testInitializeAndMakeCreateDirJobSignatures() throws Exception {
        assertThat(
                (Object)
                        DefaultImplementation.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        DefaultImplementation.class
                                .getMethod(
                                        "makeCreateDirJob",
                                        String.class,
                                        String.class,
                                        String.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testHelperMethodsExist() throws Exception {
        assertThat(
                (Object)
                        DefaultImplementation.class
                                .getDeclaredMethod("defaultTCEntry", String.class)
                                .getReturnType(),
                is(
                        (Object)
                                edu.isi.pegasus.planner.catalog.transformation
                                        .TransformationCatalogEntry.class));
        assertThat(
                (Object)
                        DefaultImplementation.class
                                .getDeclaredMethod(
                                        "getCreateDirJobExecutionSite",
                                        edu.isi.pegasus.planner.catalog.site.classes
                                                .SiteCatalogEntry.class,
                                        String.class)
                                .getReturnType(),
                is((Object) String.class));
    }
}
