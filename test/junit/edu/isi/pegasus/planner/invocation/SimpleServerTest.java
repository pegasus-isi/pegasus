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
package edu.isi.pegasus.planner.invocation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.logging.log4j.Logger;
import org.griphyn.vdl.toolkit.Toolkit;
import org.junit.jupiter.api.Test;

/** Structural tests for SimpleServer class. */
public class SimpleServerTest {

    @Test
    public void testExtendsToolkit() {
        assertThat(Toolkit.class.isAssignableFrom(SimpleServer.class), is(true));
    }

    @Test
    public void testSetTerminate() {
        boolean original = SimpleServer.getTerminate();
        SimpleServer.setTerminate(true);
        assertThat(SimpleServer.getTerminate(), is(true));
        SimpleServer.setTerminate(original);
    }

    @Test
    public void testGetTerminate() {
        SimpleServer.setTerminate(false);
        assertThat(SimpleServer.getTerminate(), is(false));
    }

    @Test
    public void testSetTerminateFalse() {
        SimpleServer.setTerminate(false);
        assertThat(SimpleServer.getTerminate(), is(false));
    }

    @Test
    public void testStaticTerminateFieldIsPublic() throws Exception {
        java.lang.reflect.Field f = SimpleServer.class.getDeclaredField("c_terminate");
        assertThat(java.lang.reflect.Modifier.isPublic(f.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isStatic(f.getModifiers()), is(true));
    }

    @Test
    public void testLoggerFieldIsPublicStaticLogger() throws Exception {
        Field field = SimpleServer.class.getDeclaredField("c_logger");

        assertThat(java.lang.reflect.Modifier.isPublic(field.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isStatic(field.getModifiers()), is(true));
        assertThat(field.getType(), is(Logger.class));
    }

    @Test
    public void testPortConstantIsPrivateStaticFinalAndExpectedValue() throws Exception {
        Field field = SimpleServer.class.getDeclaredField("port");
        field.setAccessible(true);

        assertThat(java.lang.reflect.Modifier.isPrivate(field.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isStatic(field.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isFinal(field.getModifiers()), is(true));
        assertThat(field.getInt(null), is(65533));
    }

    @Test
    public void testConstructorSignatureUsesSingleIntAndThrowsException() throws Exception {
        Constructor<SimpleServer> constructor = SimpleServer.class.getConstructor(int.class);

        assertArrayEquals(new Class<?>[] {int.class}, constructor.getParameterTypes());
        assertArrayEquals(new Class<?>[] {Exception.class}, constructor.getExceptionTypes());
    }

    @Test
    public void testShowUsageReturnsVoidAndTakesNoArguments() throws Exception {
        Method method = SimpleServer.class.getMethod("showUsage");

        assertThat(method.getReturnType(), is(void.class));
        assertThat(method.getParameterCount(), is(0));
    }
}
