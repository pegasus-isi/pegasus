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

/** Tests for JobStatusSignal invocation class. */
public class JobStatusSignalTest {

    @Test
    public void testExtendsJobStatus() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusSignal.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(JobStatusSignal.class));
    }

    @Test
    public void testDefaultConstructorDefaults() {
        JobStatusSignal j = new JobStatusSignal();
        // getSignalNumber() is the accessor method name in JobStatusSignal
        assertEquals(0, j.getSignalNumber());
        assertFalse(j.getCoreFlag());
        assertNull(j.getValue());
    }

    @Test
    public void testConstructorWithSignal() {
        JobStatusSignal j = new JobStatusSignal((short) 11);
        assertEquals(11, j.getSignalNumber());
    }

    @Test
    public void testSetAndGetSignalNumber() {
        JobStatusSignal j = new JobStatusSignal();
        j.setSignalNumber((short) 9);
        assertEquals(9, j.getSignalNumber());
    }

    @Test
    public void testSetAndGetCoreFlag() {
        JobStatusSignal j = new JobStatusSignal();
        j.setCoreFlag(true);
        assertTrue(j.getCoreFlag());
    }

    @Test
    public void testAppendValue() {
        JobStatusSignal j = new JobStatusSignal();
        j.appendValue("SIGSEGV");
        assertEquals("SIGSEGV", j.getValue());
    }
}
