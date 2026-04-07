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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for ArgVector invocation class. */
public class ArgVectorTest {

    @Test
    public void testExtendsArguments() {
        assertThat(Arguments.class.isAssignableFrom(ArgVector.class), is(true));
    }

    @Test
    public void testDefaultConstructorEmptyValue() {
        ArgVector av = new ArgVector();
        assertThat(av.getValue(), is(""));
    }

    @Test
    public void testConstructorWithExecutable() {
        ArgVector av = new ArgVector("/bin/echo");
        assertThat(av.getExecutable(), is("/bin/echo"));
        assertThat(av.getValue(), is(""));
    }

    @Test
    public void testSetValueAtPosition() {
        ArgVector av = new ArgVector();
        av.setValue(0, "first");
        av.setValue(1, "second");
        String val = av.getValue();
        assertThat(val, containsString("first"));
        assertThat(val, containsString("second"));
    }

    @Test
    public void testSetValueNegativePositionIgnored() {
        ArgVector av = new ArgVector();
        av.setValue(-1, "ignored");
        assertThat(av.getValue(), is(""));
    }

    @Test
    public void testSetValueNullBecomesEmpty() {
        ArgVector av = new ArgVector();
        av.setValue(0, null);
        // null value stored as empty string, getValue joins non-null values
        String val = av.getValue();
        assertThat(val, notNullValue());
    }

    @Test
    public void testGetValueOrdersEntriesByNumericPosition() {
        ArgVector av = new ArgVector();
        av.setValue(2, "third");
        av.setValue(0, "first");
        av.setValue(1, "second");

        assertThat(av.getValue(), is("first second third"));
    }

    @Test
    public void testNullEntryAtPositionProducesCurrentEmptyStringBehavior() {
        ArgVector av = new ArgVector();
        av.setValue(0, null);
        av.setValue(1, "value");

        assertThat(av.getValue(), is(" value"));
    }

    @Test
    public void testToXMLWithEntries() {
        ArgVector av = new ArgVector("/bin/prog");
        av.setValue(0, "arg0");
        String xml = av.toXML("");
        assertThat(xml, containsString("<argument-vector"));
        assertThat(xml, containsString("arg0"));
    }

    @Test
    public void testToXMLNoEntriesSelfClosing() {
        ArgVector av = new ArgVector("/bin/prog");
        String xml = av.toXML("");
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLStringEscapesExecutableAndArguments() {
        ArgVector av = new ArgVector("/bin/a&b");
        av.setValue(0, "1 < 2");

        String xml = av.toXML(null);

        assertThat(xml, containsString("executable=\"/bin/a&amp;b\""));
        assertThat(xml, containsString("1 &lt; 2"));
    }

    @Test
    public void testToXMLWriterCurrentlyThrowsClassCastExceptionForStoredIntegerKeys() {
        ArgVector av = new ArgVector("/bin/prog");
        av.setValue(0, "arg0");
        StringWriter sw = new StringWriter();

        assertThrows(ClassCastException.class, () -> av.toXML(sw, null, "inv"));
    }
}
