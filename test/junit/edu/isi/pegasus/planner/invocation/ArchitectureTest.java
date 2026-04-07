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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Architecture invocation class. */
public class ArchitectureTest {

    private Architecture mArchitecture;

    @BeforeEach
    public void setUp() {
        mArchitecture = new Architecture();
    }

    @AfterEach
    public void tearDown() {
        mArchitecture = null;
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Architecture.class), is(true));
    }

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Architecture.class), is(true));
    }

    @Test
    public void testDefaultConstructorInitializesNullValue() {
        assertThat(mArchitecture.getValue(), nullValue());
    }

    @Test
    public void testSetAndGetSystemName() {
        mArchitecture.setSystemName("linux");
        assertThat(mArchitecture.getSystemName(), is("linux"));
    }

    @Test
    public void testSetAndGetMachine() {
        mArchitecture.setMachine("x86_64");
        assertThat(mArchitecture.getMachine(), is("x86_64"));
    }

    @Test
    public void testSetAndGetArchMode() {
        mArchitecture.setArchMode("LP64");
        assertThat(mArchitecture.getArchMode(), is("LP64"));
    }

    @Test
    public void testSetAndGetRelease() {
        mArchitecture.setRelease("5.4.0");
        assertThat(mArchitecture.getRelease(), is("5.4.0"));
    }

    @Test
    public void testSetNodeNameNormalizesWithDomain() {
        mArchitecture.setNodeName("myhost.example.com");
        assertThat(mArchitecture.getNodeName(), is("myhost"));
        assertThat(mArchitecture.getDomainName(), is("example.com"));
    }

    @Test
    public void testSetNodeNameWithoutDomain() {
        mArchitecture.setNodeName("myhost");
        assertThat(mArchitecture.getNodeName(), is("myhost"));
        assertThat(mArchitecture.getDomainName(), nullValue());
    }

    @Test
    public void testAppendValueBuildsString() {
        mArchitecture.appendValue("hello");
        mArchitecture.appendValue(" world");
        assertThat(mArchitecture.getValue(), is("hello world"));
    }

    @Test
    public void testSetValueReplacesAppendedText() {
        mArchitecture.appendValue("hello");
        mArchitecture.setValue("replacement");

        assertThat(mArchitecture.getValue(), is("replacement"));
    }

    @Test
    public void testNormalizeDoesNotOverwriteExplicitDomainName() {
        mArchitecture.setDomainName("configured.example.org");
        mArchitecture.setNodeName("myhost.example.com");

        assertThat(mArchitecture.getNodeName(), is("myhost.example.com"));
        assertThat(mArchitecture.getDomainName(), is("configured.example.org"));
    }

    @Test
    public void testToXMLContainsSysname() throws Exception {
        mArchitecture.setSystemName("linux");
        mArchitecture.setMachine("x86_64");
        mArchitecture.setRelease("5.4");
        mArchitecture.setNodeName("host1");
        StringWriter sw = new StringWriter();
        mArchitecture.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("sysname=\"linux\""));
        assertThat(xml, containsString("<uname"));
    }

    @Test
    public void testToXMLIncludesArchmodeDomainAndTextContent() throws Exception {
        mArchitecture.setSystemName("linux");
        mArchitecture.setArchMode("LP64");
        mArchitecture.setNodeName("host1");
        mArchitecture.setRelease("5.4");
        mArchitecture.setMachine("x86_64");
        mArchitecture.setDomainName("example.com");
        mArchitecture.setValue("worker & head");
        StringWriter sw = new StringWriter();

        mArchitecture.toXML(sw, "", null);

        String xml = sw.toString();
        assertThat(xml, containsString("archmode=\"LP64\""));
        assertThat(xml, containsString("domainname=\"example.com\""));
        assertThat(xml, containsString(">worker &amp; head</uname>"));
    }

    @Test
    public void testToXMLWithoutContentUsesSelfClosingTag() throws Exception {
        mArchitecture.setSystemName("linux");
        mArchitecture.setNodeName("host1");
        mArchitecture.setRelease("5.4");
        mArchitecture.setMachine("x86_64");
        StringWriter sw = new StringWriter();

        mArchitecture.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:uname"));
        assertThat(xml, containsString("/>"));
        assertThat(xml.contains("</inv:uname>"), is(false));
    }
}
