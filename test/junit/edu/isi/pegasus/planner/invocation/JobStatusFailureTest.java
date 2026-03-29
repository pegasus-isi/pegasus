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

/** Tests for JobStatusFailure invocation class. */
public class JobStatusFailureTest {

    @Test
    public void testExtendsJobStatus() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusFailure.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(JobStatusFailure.class));
    }

    @Test
    public void testDefaultConstructorZeroError() {
        JobStatusFailure j = new JobStatusFailure();
        // getError() is the accessor for errno in JobStatusFailure
        assertEquals(0, j.getError());
        assertNull(j.getValue());
    }

    @Test
    public void testConstructorWithErrno() {
        JobStatusFailure j = new JobStatusFailure(2);
        assertEquals(2, j.getError());
    }

    @Test
    public void testConstructorWithErrnoAndValue() {
        JobStatusFailure j = new JobStatusFailure(13, "Permission denied");
        assertEquals(13, j.getError());
        assertEquals("Permission denied", j.getValue());
    }

    @Test
    public void testSetAndGetError() {
        JobStatusFailure j = new JobStatusFailure();
        j.setError(22);
        assertEquals(22, j.getError());
    }

    @Test
    public void testAppendValue() {
        JobStatusFailure j = new JobStatusFailure();
        j.appendValue("Error");
        assertEquals("Error", j.getValue());
    }
}
