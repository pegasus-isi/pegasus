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

/** Tests for JobStatusFailure invocation class. */
public class JobStatusFailureTest {

    @Test
    public void testExtendsJobStatus() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusFailure.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(JobStatusFailure.class), is(true));
    }

    @Test
    public void testDefaultConstructorZeroError() {
        JobStatusFailure j = new JobStatusFailure();
        // getError() is the accessor for errno in JobStatusFailure
        assertThat(j.getError(), is(0));
        assertThat(j.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithErrno() {
        JobStatusFailure j = new JobStatusFailure(2);
        assertThat(j.getError(), is(2));
    }

    @Test
    public void testConstructorWithErrnoAndValue() {
        JobStatusFailure j = new JobStatusFailure(13, "Permission denied");
        assertThat(j.getError(), is(13));
        assertThat(j.getValue(), is("Permission denied"));
    }

    @Test
    public void testSetAndGetError() {
        JobStatusFailure j = new JobStatusFailure();
        j.setError(22);
        assertThat(j.getError(), is(22));
    }

    @Test
    public void testAppendValue() {
        JobStatusFailure j = new JobStatusFailure();
        j.appendValue("Error");
        assertThat(j.getValue(), is("Error"));
    }

    @Test
    public void testAppendNullIsNoop() {
        JobStatusFailure j = new JobStatusFailure(5, "errno");
        j.appendValue(null);

        assertThat(j.getValue(), is("errno"));
    }

    @Test
    public void testSetValueReplacesAppendedContent() {
        JobStatusFailure j = new JobStatusFailure();
        j.appendValue("old");
        j.setValue("new");

        assertThat(j.getValue(), is("new"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        JobStatusFailure j = new JobStatusFailure(1, "failure");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> j.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLStringUsesSelfClosingTagWhenValueNull() {
        JobStatusFailure j = new JobStatusFailure(7);

        String xml = j.toXML("");

        assertThat(xml, containsString("<failure"));
        assertThat(xml, containsString("error=\"7\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesValue() throws Exception {
        JobStatusFailure j = new JobStatusFailure(13, "A&B < C");
        StringWriter sw = new StringWriter();

        j.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:failure"));
        assertThat(xml, containsString("error=\"13\""));
        assertThat(xml, containsString(">A&amp;B &lt; C</inv:failure>"));
    }
}
