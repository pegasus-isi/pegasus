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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for StatCall invocation class. */
public class StatCallTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(StatCall.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullHandle() {
        StatCall sc = new StatCall();
        assertThat(sc.getHandle(), is(nullValue()));
    }

    @Test
    public void testConstructorWithHandle() {
        StatCall sc = new StatCall("stdin");
        assertThat(sc.getHandle(), is("stdin"));
    }

    @Test
    public void testSetAndGetHandle() {
        StatCall sc = new StatCall();
        sc.setHandle("stdout");
        assertThat(sc.getHandle(), is("stdout"));
    }

    @Test
    public void testSetAndGetLFN() {
        StatCall sc = new StatCall();
        sc.setLFN("output.txt");
        assertThat(sc.getLFN(), is("output.txt"));
    }

    @Test
    public void testSetAndGetError() {
        StatCall sc = new StatCall();
        sc.setError(2);
        assertThat(sc.getError(), is(2));
    }

    @Test
    public void testSetAndGetStatInfo() {
        StatCall sc = new StatCall();
        StatInfo si = new StatInfo();
        sc.setStatInfo(si);
        assertThat(sc.getStatInfo(), is(notNullValue()));
    }

    @Test
    public void testSetDataString() {
        StatCall sc = new StatCall();
        sc.setData("some content");
        assertThat(sc.getData(), is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorNullFileAndData() {
        StatCall sc = new StatCall();
        assertThat(sc.getFile(), is(nullValue()));
        assertThat(sc.getData(), is(nullValue()));
    }

    @Test
    public void testSetAndGetFile() {
        StatCall sc = new StatCall();
        Regular file = new Regular("stdout.log");

        sc.setFile(file);

        assertThat(sc.getFile(), is(sameInstance(file)));
    }

    @Test
    public void testSetDataObjectUsesProvidedInstance() {
        StatCall sc = new StatCall();
        Data data = new Data("abcd");

        sc.setData(data);

        assertThat(sc.getData(), is(sameInstance(data)));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        StatCall sc = new StatCall();

        IOException exception =
                assertThrows(IOException.class, () -> sc.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLUsesNamespaceAndNestedElements() throws IOException {
        StatCall sc = new StatCall("stdout");
        sc.setLFN("output.txt");
        sc.setError(2);
        sc.setFile(new Regular("output.txt"));
        sc.setStatInfo(new StatInfo());
        sc.setData("abcd");

        StringWriter writer = new StringWriter();
        sc.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:statcall"), is(true));
        assertThat(xml, containsString("error=\"2\""));
        assertThat(xml, containsString("id=\"stdout\""));
        assertThat(xml, containsString("lfn=\"output.txt\""));
        assertThat(xml, containsString("<inv:file"));
        assertThat(xml, containsString("<inv:statinfo"));
        assertThat(xml, containsString("<inv:data"));
        assertThat(xml.endsWith("  </inv:statcall>" + System.lineSeparator()), is(true));
    }
}
