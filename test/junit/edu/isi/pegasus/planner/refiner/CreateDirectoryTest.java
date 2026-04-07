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
package edu.isi.pegasus.planner.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.refiner.createdir.Implementation;
import edu.isi.pegasus.planner.refiner.createdir.Strategy;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for CreateDirectory refiner. */
public class CreateDirectoryTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(CreateDirectory.class), is(true));
    }

    @Test
    public void testPackageName() {
        assertThat(CreateDirectory.PACKAGE_NAME, is("edu.isi.pegasus.planner.refiner.createdir"));
    }

    @Test
    public void testHasLoadStaticMethod() throws Exception {
        assertThat(
                CreateDirectory.class.getMethod(
                        "loadCreateDirectoryStraegyInstance",
                        edu.isi.pegasus.planner.classes.PegasusBag.class),
                notNullValue());
    }

    @Test
    public void testHasProtectedPegasusBagConstructor() throws Exception {
        Constructor<CreateDirectory> constructor =
                CreateDirectory.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(Modifier.isProtected(constructor.getModifiers()), is(true));
    }

    @Test
    public void testImplementationLoaderMethodSignature() throws Exception {
        Method method =
                CreateDirectory.class.getMethod(
                        "loadCreateDirectoryImplementationInstance", PegasusBag.class);
        assertThat((Object) method.getReturnType(), is((Object) Implementation.class));
    }

    @Test
    public void testStrategyLoaderMethodSignature() throws Exception {
        Method method =
                CreateDirectory.class.getMethod(
                        "loadCreateDirectoryStraegyInstance", PegasusBag.class);
        assertThat((Object) method.getReturnType(), is((Object) Strategy.class));
    }

    @Test
    public void testAddCreateDirectoryNodesReturnsADag() throws Exception {
        Method method = CreateDirectory.class.getMethod("addCreateDirectoryNodes", ADag.class);
        assertThat((Object) method.getReturnType(), is((Object) ADag.class));
    }
}
