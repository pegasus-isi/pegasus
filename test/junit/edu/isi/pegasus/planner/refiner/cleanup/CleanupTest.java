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
package edu.isi.pegasus.planner.refiner.cleanup;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for Cleanup (CleanupImplementation using pegasus-transfer). */
public class CleanupTest {

    @Test
    public void testImplementsCleanupImplementation() {
        assertThat(CleanupImplementation.class.isAssignableFrom(Cleanup.class), is(true));
    }

    @Test
    public void testTransformationNamespace() {
        assertThat(Cleanup.TRANSFORMATION_NAMESPACE, is("pegasus"));
    }

    @Test
    public void testTransformationName() {
        assertThat(Cleanup.TRANSFORMATION_NAME, is("cleanup"));
    }

    @Test
    public void testDerivationNamespace() {
        assertThat(Cleanup.DERIVATION_NAMESPACE, is("pegasus"));
    }

    @Test
    public void testDefaultConstructor() {
        Cleanup c = new Cleanup();
        assertThat(c, notNullValue());
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(Cleanup.TRANSFORMATION_VERSION, nullValue());
        assertThat(Cleanup.DERIVATION_NAME, is("cleanup"));
        assertThat(Cleanup.DERIVATION_VERSION, nullValue());
        assertThat(Cleanup.EXECUTABLE_BASENAME, is("pegasus-transfer"));
        assertThat(Cleanup.DESCRIPTION, containsString("transfer client"));
    }

    @Test
    public void testGetCompleteTransformationName() {
        assertThat(Cleanup.getCompleteTranformationName(), is("pegasus::cleanup"));
    }

    @Test
    public void testHasThreeArgumentCreateCleanupJobMethod() throws Exception {
        assertThat(
                (Object)
                        Cleanup.class
                                .getMethod(
                                        "createCleanupJob",
                                        String.class,
                                        java.util.List.class,
                                        edu.isi.pegasus.planner.classes.Job.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testHasFourArgumentCreateCleanupJobMethod() throws Exception {
        assertThat(
                (Object)
                        Cleanup.class
                                .getMethod(
                                        "createCleanupJob",
                                        String.class,
                                        java.util.List.class,
                                        edu.isi.pegasus.planner.classes.Job.class,
                                        String.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testInitializeMethodReturnsVoid() throws Exception {
        assertThat(
                (Object)
                        Cleanup.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
    }
}
