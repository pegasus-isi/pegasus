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

import org.griphyn.vdl.toolkit.Toolkit;
import org.junit.jupiter.api.Test;

/** Structural tests for SimpleServer class. */
public class SimpleServerTest {

    @Test
    public void testExtendsToolkit() {
        assertTrue(Toolkit.class.isAssignableFrom(SimpleServer.class));
    }

    @Test
    public void testSetTerminate() {
        boolean original = SimpleServer.getTerminate();
        SimpleServer.setTerminate(true);
        assertTrue(SimpleServer.getTerminate());
        SimpleServer.setTerminate(original);
    }

    @Test
    public void testGetTerminate() {
        SimpleServer.setTerminate(false);
        assertFalse(SimpleServer.getTerminate());
    }

    @Test
    public void testSetTerminateFalse() {
        SimpleServer.setTerminate(false);
        assertFalse(SimpleServer.getTerminate());
    }

    @Test
    public void testStaticTerminateFieldIsPublic() throws Exception {
        java.lang.reflect.Field f = SimpleServer.class.getDeclaredField("c_terminate");
        assertTrue(java.lang.reflect.Modifier.isPublic(f.getModifiers()));
        assertTrue(java.lang.reflect.Modifier.isStatic(f.getModifiers()));
    }
}
