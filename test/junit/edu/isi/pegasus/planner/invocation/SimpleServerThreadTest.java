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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for SimpleServerThread class. */
public class SimpleServerThreadTest {

    @Test
    public void testExtendsThread() {
        assertTrue(Thread.class.isAssignableFrom(SimpleServerThread.class));
    }

    @Test
    public void testStaticCountFieldExists() throws Exception {
        java.lang.reflect.Field f = SimpleServerThread.class.getDeclaredField("c_count");
        assertTrue(java.lang.reflect.Modifier.isStatic(f.getModifiers()));
    }

    @Test
    public void testStaticCdoneFieldExists() throws Exception {
        java.lang.reflect.Field f = SimpleServerThread.class.getDeclaredField("c_cdone");
        assertTrue(java.lang.reflect.Modifier.isStatic(f.getModifiers()));
    }

    @Test
    public void testHasRunMethod() throws Exception {
        java.lang.reflect.Method m = SimpleServerThread.class.getDeclaredMethod("run");
        assertNotNull(m);
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(SimpleServerThread.class.getModifiers()));
    }
}
