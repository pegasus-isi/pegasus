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

/** Tests for JobStatusSignal invocation class. */
public class JobStatusSignalTest {

    @Test
    public void testExtendsJobStatus() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusSignal.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(JobStatusSignal.class), is(true));
    }

    @Test
    public void testDefaultConstructorDefaults() {
        JobStatusSignal j = new JobStatusSignal();
        // getSignalNumber() is the accessor method name in JobStatusSignal
        assertThat(j.getSignalNumber(), is((short) 0));
        assertThat(j.getCoreFlag(), is(false));
        assertThat(j.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithSignal() {
        JobStatusSignal j = new JobStatusSignal((short) 11);
        assertThat(j.getSignalNumber(), is((short) 11));
    }

    @Test
    public void testSetAndGetSignalNumber() {
        JobStatusSignal j = new JobStatusSignal();
        j.setSignalNumber((short) 9);
        assertThat(j.getSignalNumber(), is((short) 9));
    }

    @Test
    public void testSetAndGetCoreFlag() {
        JobStatusSignal j = new JobStatusSignal();
        j.setCoreFlag(true);
        assertThat(j.getCoreFlag(), is(true));
    }

    @Test
    public void testAppendValue() {
        JobStatusSignal j = new JobStatusSignal();
        j.appendValue("SIGSEGV");
        assertThat(j.getValue(), is("SIGSEGV"));
    }

    @Test
    public void testConstructorWithSignalCoreAndValueCurrentCoreBehavior() {
        JobStatusSignal j = new JobStatusSignal((short) 11, true, "SIGSEGV");

        assertThat(j.getSignalNumber(), is((short) 11));
        assertThat(j.getCoreFlag(), is(false));
        assertThat(j.getValue(), is("SIGSEGV"));
    }

    @Test
    public void testAppendNullIsNoop() {
        JobStatusSignal j = new JobStatusSignal((short) 9, false, "SIGKILL");
        j.appendValue(null);

        assertThat(j.getValue(), is("SIGKILL"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        JobStatusSignal j = new JobStatusSignal();
        j.appendValue("old");
        j.setValue("new");

        assertThat(j.getValue(), is("new"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        JobStatusSignal j = new JobStatusSignal((short) 6, true, "SIGABRT");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> j.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLStringUsesSelfClosingTagWhenValueNull() {
        JobStatusSignal j = new JobStatusSignal((short) 15, true);

        String xml = j.toXML("");

        assertThat(xml, containsString("<signalled"));
        assertThat(xml, containsString("signal=\"15\""));
        assertThat(xml, containsString("corefile=\"true\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesValue() throws Exception {
        JobStatusSignal j = new JobStatusSignal((short) 11, false);
        j.setValue("A&B < C");
        StringWriter sw = new StringWriter();

        j.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:signalled"));
        assertThat(xml, containsString("signal=\"11\""));
        assertThat(xml, containsString("corefile=\"false\""));
        assertThat(xml, containsString(">A&amp;B &lt; C</inv:signalled>"));
    }
}
