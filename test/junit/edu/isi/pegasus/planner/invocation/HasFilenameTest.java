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

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for HasFilename interface structure. */
public class HasFilenameTest {

    @Test
    public void testIsInterface() {
        assertTrue(HasFilename.class.isInterface());
    }

    @Test
    public void testHasGetFilenameMethod() throws Exception {
        Method m = HasFilename.class.getMethod("getFilename");
        assertNotNull(m);
        assertEquals(String.class, m.getReturnType());
    }

    @Test
    public void testHasSetFilenameMethod() throws Exception {
        Method m = HasFilename.class.getMethod("setFilename", String.class);
        assertNotNull(m);
    }

    @Test
    public void testTemporaryImplementsInterface() {
        assertTrue(HasFilename.class.isAssignableFrom(Temporary.class));
    }

    @Test
    public void testFifoImplementsInterface() {
        assertTrue(HasFilename.class.isAssignableFrom(Fifo.class));
    }

    @Test
    public void testTemporarySetAndGetFilename() {
        Temporary t = new Temporary("/tmp/test.tmp", 1);
        assertEquals("/tmp/test.tmp", t.getFilename());
    }

    @Test
    public void testFifoSetAndGetFilename() {
        Fifo f = new Fifo("/tmp/mypipe", 3);
        assertEquals("/tmp/mypipe", f.getFilename());
    }
}
