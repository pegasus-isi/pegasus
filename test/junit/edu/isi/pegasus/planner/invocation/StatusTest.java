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
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Status invocation class. */
public class StatusTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Status.class), is(true));
    }

    @Test
    public void testDefaultConstructorZeroStatus() {
        Status s = new Status();
        assertThat(s.getStatus(), is(0));
    }

    @Test
    public void testDefaultConstructorNullJobStatus() {
        Status s = new Status();
        assertThat(s.getJobStatus(), is(nullValue()));
    }

    @Test
    public void testConstructorWithRaw() {
        Status s = new Status(42);
        assertThat(s.getStatus(), is(42));
    }

    @Test
    public void testConstructorWithRawAndJobStatus() {
        JobStatusRegular jsr = new JobStatusRegular();
        Status s = new Status(0, jsr);
        assertThat(s.getStatus(), is(0));
        assertThat(s.getJobStatus(), is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testSetAndGetStatus() {
        Status s = new Status();
        s.setStatus(255);
        assertThat(s.getStatus(), is(255));
    }

    @Test
    public void testSetAndGetJobStatus() {
        Status s = new Status();
        JobStatusRegular jsr = new JobStatusRegular();
        s.setJobStatus(jsr);
        assertThat(s.getJobStatus(), is(sameInstance(jsr)));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Status s = new Status();

        IOException exception =
                assertThrows(IOException.class, () -> s.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLUsesNamespaceAndNestedJobStatus() throws IOException {
        JobStatusRegular regular = new JobStatusRegular();
        regular.setExitCode((short) 5);
        Status status = new Status(255, regular);

        StringWriter writer = new StringWriter();
        status.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:status raw=\"255\">"), is(true));
        assertThat(xml, containsString("<inv:regular"));
        assertThat(xml, containsString("exitcode=\"5\""));
        assertThat(xml.endsWith("</inv:status>" + System.lineSeparator()), is(true));
    }

    @Test
    public void testToXMLWithoutJobStatusThrowsCurrentRuntimeException() {
        Status status = new Status(0);

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class, () -> status.toXML(new StringWriter(), "", null));

        assertThat(exception.getMessage(), is("unknown state of job status"));
    }
}
