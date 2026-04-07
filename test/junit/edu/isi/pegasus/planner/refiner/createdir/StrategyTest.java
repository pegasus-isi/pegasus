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

/** Structural tests for createdir Strategy interface. */
public class StrategyTest {

    @Test
    public void testVersionConstant() {
        assertThat(Strategy.VERSION, is("1.0"));
    }

    @Test
    public void testHourGlassImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(HourGlass.class), is(true));
    }

    @Test
    public void testMinimalImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(Minimal.class), is(true));
    }

    @Test
    public void testTentaclesImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(Tentacles.class), is(true));
    }

    @Test
    public void testHasAddCreateDirectoryNodesMethod() throws Exception {
        assertThat(
                Strategy.class.getMethod(
                        "addCreateDirectoryNodes", edu.isi.pegasus.planner.classes.ADag.class),
                notNullValue());
    }

    @Test
    public void testStrategyIsInterface() {
        assertThat(Strategy.class.isInterface(), is(true));
    }

    @Test
    public void testInitializeAndReturnTypes() throws Exception {
        assertThat(
                Strategy.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        Implementation.class),
                notNullValue());
        assertThat(
                (Object)
                        Strategy.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                                        Implementation.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        Strategy.class
                                .getMethod(
                                        "addCreateDirectoryNodes",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.ADag.class));
    }

    @Test
    public void testStrategyDeclaresExpectedMethods() {
        assertThat(Strategy.class.getDeclaredMethods().length, is(2));
    }
}
