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
import org.junit.jupiter.api.Test;

/** Structural tests for SimpleServerThread class. */
public class SimpleServerThreadTest {

    @Test
    public void testExtendsThread() {
        assertThat(Thread.class.isAssignableFrom(SimpleServerThread.class), is(true));
    }

    @Test
    public void testStaticCountFieldExists() throws Exception {
        java.lang.reflect.Field f = SimpleServerThread.class.getDeclaredField("c_count");
        assertThat(java.lang.reflect.Modifier.isStatic(f.getModifiers()), is(true));
    }

    @Test
    public void testStaticCdoneFieldExists() throws Exception {
        java.lang.reflect.Field f = SimpleServerThread.class.getDeclaredField("c_cdone");
        assertThat(java.lang.reflect.Modifier.isStatic(f.getModifiers()), is(true));
    }

    @Test
    public void testHasRunMethod() throws Exception {
        java.lang.reflect.Method m = SimpleServerThread.class.getDeclaredMethod("run");
        assertThat(m, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testLoggerFieldExistsAndIsStatic() throws Exception {
        Field field = SimpleServerThread.class.getDeclaredField("c_logger");
        assertThat(java.lang.reflect.Modifier.isStatic(field.getModifiers()), is(true));
    }

    @Test
    public void testConstructorSignatureUsesSimpleServerAndSocket() throws Exception {
        Constructor<SimpleServerThread> constructor =
                SimpleServerThread.class.getConstructor(SimpleServer.class, java.net.Socket.class);

        assertArrayEquals(
                new Class<?>[] {SimpleServer.class, java.net.Socket.class},
                constructor.getParameterTypes());
    }

    @Test
    public void testLogMethodExistsWithExpectedSignature() throws Exception {
        java.lang.reflect.Method method =
                SimpleServerThread.class.getDeclaredMethod(
                        "log", org.apache.logging.log4j.Level.class, String.class);

        assertThat(method.getReturnType(), is(void.class));
        assertArrayEquals(
                new Class<?>[] {org.apache.logging.log4j.Level.class, String.class},
                method.getParameterTypes());
    }

    @Test
    public void testRemoteSocketAndServerFieldsExistWithExpectedTypes() throws Exception {
        Field remoteField = SimpleServerThread.class.getDeclaredField("m_remote");
        Field socketField = SimpleServerThread.class.getDeclaredField("m_socket");
        Field serverField = SimpleServerThread.class.getDeclaredField("m_server");

        assertThat(remoteField.getType(), is(String.class));
        assertThat(socketField.getType(), is(java.net.Socket.class));
        assertThat(serverField.getType(), is(SimpleServer.class));
    }
}
