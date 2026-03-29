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

/** Tests for Fifo invocation class. */
public class FifoTest {

    @Test
    public void testExtendsTemporary() {
        assertTrue(Temporary.class.isAssignableFrom(Fifo.class));
    }

    @Test
    public void testDefaultConstructorZeroCounts() {
        Fifo f = new Fifo();
        assertEquals(0, f.getCount());
        // input/output sizes start at 0
        assertEquals(0, f.getInputSize());
        assertEquals(0, f.getOutputSize());
    }

    @Test
    public void testConstructorWithFilenameAndDescriptor() {
        Fifo f = new Fifo("/tmp/mypipe", 5);
        assertEquals("/tmp/mypipe", f.getFilename());
        assertEquals(5, f.getDescriptor());
    }

    @Test
    public void testSetAndGetCount() {
        Fifo f = new Fifo();
        f.setCount(10);
        assertEquals(10, f.getCount());
    }

    @Test
    public void testSetAndGetInputSize() {
        Fifo f = new Fifo();
        f.setInputSize(1024L);
        assertEquals(1024L, f.getInputSize());
    }

    @Test
    public void testSetAndGetOutputSize() {
        Fifo f = new Fifo();
        f.setOutputSize(2048L);
        assertEquals(2048L, f.getOutputSize());
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertTrue(HasDescriptor.class.isAssignableFrom(Fifo.class));
    }

    @Test
    public void testImplementsHasFilename() {
        assertTrue(HasFilename.class.isAssignableFrom(Fifo.class));
    }
}
