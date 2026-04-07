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

/** Structural tests for Tentacles createdir strategy. */
public class TentaclesTest {

    @Test
    public void testExtendsAbstractStrategy() {
        assertThat(AbstractStrategy.class.isAssignableFrom(Tentacles.class), is(true));
    }

    @Test
    public void testImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(Tentacles.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        Tentacles t = new Tentacles();
        assertThat(t, notNullValue());
    }

    @Test
    public void testHasAddCreateDirectoryNodesMethod() throws Exception {
        assertThat(
                Tentacles.class.getMethod(
                        "addCreateDirectoryNodes", edu.isi.pegasus.planner.classes.ADag.class),
                notNullValue());
    }

    @Test
    public void testPublicMethodReturnTypes() throws Exception {
        assertThat(
                (Object)
                        Tentacles.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                                        Implementation.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        Tentacles.class
                                .getMethod(
                                        "addCreateDirectoryNodes",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.ADag.class));
    }

    @Test
    public void testTentaclesDeclaresExpectedMethods() {
        assertThat(Tentacles.class.getDeclaredMethods().length, is(2));
    }
}
