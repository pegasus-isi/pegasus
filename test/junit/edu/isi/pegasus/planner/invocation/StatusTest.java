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

/** Tests for Status invocation class. */
public class StatusTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Status.class));
    }

    @Test
    public void testDefaultConstructorZeroStatus() {
        Status s = new Status();
        assertEquals(0, s.getStatus());
    }

    @Test
    public void testDefaultConstructorNullJobStatus() {
        Status s = new Status();
        assertNull(s.getJobStatus());
    }

    @Test
    public void testConstructorWithRaw() {
        Status s = new Status(42);
        assertEquals(42, s.getStatus());
    }

    @Test
    public void testConstructorWithRawAndJobStatus() {
        JobStatusRegular jsr = new JobStatusRegular();
        Status s = new Status(0, jsr);
        assertEquals(0, s.getStatus());
        assertNotNull(s.getJobStatus());
    }

    @Test
    public void testSetAndGetStatus() {
        Status s = new Status();
        s.setStatus(255);
        assertEquals(255, s.getStatus());
    }

    @Test
    public void testSetAndGetJobStatus() {
        Status s = new Status();
        JobStatusRegular jsr = new JobStatusRegular();
        s.setJobStatus(jsr);
        assertSame(jsr, s.getJobStatus());
    }
}
