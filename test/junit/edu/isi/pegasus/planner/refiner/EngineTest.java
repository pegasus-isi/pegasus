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

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for Engine abstract class. */
public class EngineTest {

    @Test
    public void testRegistrationUniverseConstant() {
        assertThat(Engine.REGISTRATION_UNIVERSE, is("registration"));
    }

    @Test
    public void testTransferUniverseConstant() {
        assertThat(Engine.TRANSFER_UNIVERSE, is("transfer"));
    }

    @Test
    public void testInterPoolEngineExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(InterPoolEngine.class), is(true));
    }

    @Test
    public void testTransferEngineExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(TransferEngine.class), is(true));
    }

    @Test
    public void testEngineIsAbstract() {
        assertThat(Modifier.isAbstract(Engine.class.getModifiers()), is(true));
    }

    @Test
    public void testHasPegasusBagConstructor() throws Exception {
        Constructor<Engine> constructor = Engine.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
    }

    @Test
    public void testLoadPropertiesReturnsVoid() throws Exception {
        Method method = Engine.class.getDeclaredMethod("loadProperties");
        assertThat((Object) method.getReturnType(), is((Object) void.class));
    }

    @Test
    public void testProtectedComplainMethodsExist() throws Exception {
        Method shortMethod =
                Engine.class.getDeclaredMethod(
                        "complainForHeadNodeURLPrefix",
                        String.class,
                        String.class,
                        FileServer.OPERATION.class);
        Method longMethod =
                Engine.class.getDeclaredMethod(
                        "complainForHeadNodeURLPrefix",
                        String.class,
                        String.class,
                        FileServer.OPERATION.class,
                        Job.class);
        assertThat(Modifier.isProtected(shortMethod.getModifiers()), is(true));
        assertThat(Modifier.isProtected(longMethod.getModifiers()), is(true));
    }
}
