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

/** Tests for JobStatusRegular invocation class. */
public class JobStatusRegularTest {

    @Test
    public void testExtendsJobStatus() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusRegular.class));
    }

    @Test
    public void testDefaultConstructorExitCodeZero() {
        JobStatusRegular j = new JobStatusRegular();
        assertEquals(0, j.getExitCode());
    }

    @Test
    public void testConstructorWithExitCode() {
        JobStatusRegular j = new JobStatusRegular((short) 42);
        assertEquals(42, j.getExitCode());
    }

    @Test
    public void testSetAndGetExitCode() {
        JobStatusRegular j = new JobStatusRegular();
        j.setExitCode((short) 1);
        assertEquals(1, j.getExitCode());
    }

    @Test
    public void testToXMLContainsExitcode() {
        JobStatusRegular j = new JobStatusRegular((short) 0);
        String xml = j.toXML("");
        assertTrue(xml.contains("exitcode=\"0\""));
        assertTrue(xml.contains("<regular"));
    }

    @Test
    public void testToXMLNonZeroExitCode() {
        JobStatusRegular j = new JobStatusRegular((short) 127);
        String xml = j.toXML("");
        assertTrue(xml.contains("exitcode=\"127\""));
    }

    @Test
    public void testToXMLSelfClosingTag() {
        JobStatusRegular j = new JobStatusRegular((short) 0);
        String xml = j.toXML("");
        assertTrue(xml.contains("/>"));
    }
}
