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

/** Structural tests for createdir Implementation interface. */
public class ImplementationTest {

    @Test
    public void testVersionConstant() {
        assertThat(Implementation.VERSION, is("1.1"));
    }

    @Test
    public void testDefaultImplementationImplementsInterface() {
        assertThat(Implementation.class.isAssignableFrom(DefaultImplementation.class), is(true));
    }

    @Test
    public void testHasMakeCreateDirJobMethod() throws Exception {
        assertThat(
                Implementation.class.getMethod(
                        "makeCreateDirJob", String.class, String.class, String.class),
                notNullValue());
    }

    @Test
    public void testHasInitializeMethod() throws Exception {
        assertThat(
                Implementation.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class),
                notNullValue());
    }

    @Test
    public void testImplementationIsInterface() {
        assertThat(Implementation.class.isInterface(), is(true));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                (Object)
                        Implementation.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        Implementation.class
                                .getMethod(
                                        "makeCreateDirJob",
                                        String.class,
                                        String.class,
                                        String.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testImplementationDeclaresExpectedMethods() {
        assertThat(Implementation.class.getDeclaredMethods().length, is(2));
    }
}
