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

/** Tests for Fifo invocation class. */
public class FifoTest {

    @Test
    public void testExtendsTemporary() {
        assertThat(Temporary.class.isAssignableFrom(Fifo.class), is(true));
    }

    @Test
    public void testDefaultConstructorZeroCounts() {
        Fifo f = new Fifo();
        assertThat(f.getCount(), is(0));
        // input/output sizes start at 0
        assertThat(f.getInputSize(), is(0L));
        assertThat(f.getOutputSize(), is(0L));
    }

    @Test
    public void testConstructorWithFilenameAndDescriptor() {
        Fifo f = new Fifo("/tmp/mypipe", 5);
        assertThat(f.getFilename(), is("/tmp/mypipe"));
        assertThat(f.getDescriptor(), is(5));
    }

    @Test
    public void testSetAndGetCount() {
        Fifo f = new Fifo();
        f.setCount(10);
        assertThat(f.getCount(), is(10));
    }

    @Test
    public void testSetAndGetInputSize() {
        Fifo f = new Fifo();
        f.setInputSize(1024L);
        assertThat(f.getInputSize(), is(1024L));
    }

    @Test
    public void testSetAndGetOutputSize() {
        Fifo f = new Fifo();
        f.setOutputSize(2048L);
        assertThat(f.getOutputSize(), is(2048L));
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertThat(HasDescriptor.class.isAssignableFrom(Fifo.class), is(true));
    }

    @Test
    public void testImplementsHasFilename() {
        assertThat(HasFilename.class.isAssignableFrom(Fifo.class), is(true));
    }

    @Test
    public void testToXMLWithoutValueUsesSelfClosingTag() throws Exception {
        Fifo f = new Fifo("/tmp/mypipe", 5);
        f.setCount(10);
        f.setInputSize(1024L);
        f.setOutputSize(2048L);
        StringWriter sw = new StringWriter();

        f.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:fifo"));
        assertThat(xml, containsString("name=\"/tmp/mypipe\""));
        assertThat(xml, containsString("descriptor=\"5\""));
        assertThat(xml, containsString("count=\"10\""));
        assertThat(xml, containsString("rsize=\"1024\""));
        assertThat(xml, containsString("wsize=\"2048\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLWithHexValueIncludesContentAndClosingTag() throws Exception {
        Fifo f = new Fifo("/tmp/mypipe", 1);
        f.setValue("deadbeef");
        StringWriter sw = new StringWriter();

        f.toXML(sw, "", null);

        String xml = sw.toString();
        assertThat(xml, containsString("<fifo"));
        assertThat(xml, containsString("descriptor=\"1\""));
        assertThat(xml, containsString(">deadbeef</fifo>"));
    }
}
