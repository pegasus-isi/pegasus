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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for JobStatusSuspend invocation class. */
public class JobStatusSuspendTest {

    @Test
    public void testExtendsJobStatus() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusSuspend.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(JobStatusSuspend.class), is(true));
    }

    @Test
    public void testDefaultConstructorDefaults() {
        JobStatusSuspend j = new JobStatusSuspend();
        // getSignalNumber() is the accessor method name in JobStatusSuspend
        assertThat(j.getSignalNumber(), is((short) 0));
        assertThat(j.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithSignal() {
        JobStatusSuspend j = new JobStatusSuspend((short) 19);
        assertThat(j.getSignalNumber(), is((short) 19));
    }

    @Test
    public void testConstructorWithSignalAndValue() {
        JobStatusSuspend j = new JobStatusSuspend((short) 17, "SIGCHLD");
        assertThat(j.getSignalNumber(), is((short) 17));
        assertThat(j.getValue(), is("SIGCHLD"));
    }

    @Test
    public void testSetAndGetSignalNumber() {
        JobStatusSuspend j = new JobStatusSuspend();
        j.setSignalNumber((short) 20);
        assertThat(j.getSignalNumber(), is((short) 20));
    }

    @Test
    public void testAppendValue() {
        JobStatusSuspend j = new JobStatusSuspend();
        j.appendValue("suspended");
        assertThat(j.getValue(), is("suspended"));
    }

    @Test
    public void testAppendNullIsNoop() {
        JobStatusSuspend j = new JobStatusSuspend((short) 19, "SIGSTOP");
        j.appendValue(null);

        assertThat(j.getValue(), is("SIGSTOP"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        JobStatusSuspend j = new JobStatusSuspend();
        j.appendValue("old");
        j.setValue("new");

        assertThat(j.getValue(), is("new"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        JobStatusSuspend j = new JobStatusSuspend((short) 19, "SIGSTOP");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> j.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLStringUsesSelfClosingTagWhenValueNull() {
        JobStatusSuspend j = new JobStatusSuspend((short) 19);

        String xml = j.toXML("");

        assertThat(xml, containsString("<suspended"));
        assertThat(xml, containsString("signal=\"19\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesValue() throws Exception {
        JobStatusSuspend j = new JobStatusSuspend((short) 20, "A&B < C");
        StringWriter sw = new StringWriter();

        j.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:suspended"));
        assertThat(xml, containsString("signal=\"20\""));
        assertThat(xml, containsString(">A&amp;B &lt; C</inv:suspended>"));
    }
}
