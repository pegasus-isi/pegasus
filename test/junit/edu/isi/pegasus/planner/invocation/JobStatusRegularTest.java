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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for JobStatusRegular invocation class. */
public class JobStatusRegularTest {

    @Test
    public void testExtendsJobStatus() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusRegular.class), is(true));
    }

    @Test
    public void testDefaultConstructorExitCodeZero() {
        JobStatusRegular j = new JobStatusRegular();
        assertThat(j.getExitCode(), is((short) 0));
    }

    @Test
    public void testConstructorWithExitCode() {
        JobStatusRegular j = new JobStatusRegular((short) 42);
        assertThat(j.getExitCode(), is((short) 42));
    }

    @Test
    public void testSetAndGetExitCode() {
        JobStatusRegular j = new JobStatusRegular();
        j.setExitCode((short) 1);
        assertThat(j.getExitCode(), is((short) 1));
    }

    @Test
    public void testToXMLContainsExitcode() {
        JobStatusRegular j = new JobStatusRegular((short) 0);
        String xml = j.toXML("");
        assertThat(xml, containsString("exitcode=\"0\""));
        assertThat(xml, containsString("<regular"));
    }

    @Test
    public void testToXMLNonZeroExitCode() {
        JobStatusRegular j = new JobStatusRegular((short) 127);
        String xml = j.toXML("");
        assertThat(xml, containsString("exitcode=\"127\""));
    }

    @Test
    public void testToXMLSelfClosingTag() {
        JobStatusRegular j = new JobStatusRegular((short) 0);
        String xml = j.toXML("");
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        JobStatusRegular j = new JobStatusRegular((short) 5);
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> j.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLWriterUsesNamespacePrefix() throws Exception {
        JobStatusRegular j = new JobStatusRegular((short) 12);
        StringWriter sw = new StringWriter();

        j.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, is("<inv:regular exitcode=\"12\"/>"));
    }

    @Test
    public void testNegativeExitCodeIsSerialized() {
        JobStatusRegular j = new JobStatusRegular((short) -1);

        String xml = j.toXML("");

        assertThat(xml, containsString("exitcode=\"-1\""));
    }
}
