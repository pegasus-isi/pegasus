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

/** Tests for JobStatusSuspend invocation class. */
public class JobStatusSuspendTest {

    @Test
    public void testExtendsJobStatus() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusSuspend.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(JobStatusSuspend.class));
    }

    @Test
    public void testDefaultConstructorDefaults() {
        JobStatusSuspend j = new JobStatusSuspend();
        // getSignalNumber() is the accessor method name in JobStatusSuspend
        assertEquals(0, j.getSignalNumber());
        assertNull(j.getValue());
    }

    @Test
    public void testConstructorWithSignal() {
        JobStatusSuspend j = new JobStatusSuspend((short) 19);
        assertEquals(19, j.getSignalNumber());
    }

    @Test
    public void testConstructorWithSignalAndValue() {
        JobStatusSuspend j = new JobStatusSuspend((short) 17, "SIGCHLD");
        assertEquals(17, j.getSignalNumber());
        assertEquals("SIGCHLD", j.getValue());
    }

    @Test
    public void testSetAndGetSignalNumber() {
        JobStatusSuspend j = new JobStatusSuspend();
        j.setSignalNumber((short) 20);
        assertEquals(20, j.getSignalNumber());
    }

    @Test
    public void testAppendValue() {
        JobStatusSuspend j = new JobStatusSuspend();
        j.appendValue("suspended");
        assertEquals("suspended", j.getValue());
    }
}
