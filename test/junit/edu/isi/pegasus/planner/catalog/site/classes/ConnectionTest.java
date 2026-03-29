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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ConnectionTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorEmptyKeyAndValue() {
        Connection c = new Connection();
        assertEquals("", c.getKey());
        assertEquals("", c.getValue());
    }

    @Test
    public void testOverloadedConstructorSetsKeyAndValue() {
        Connection c = new Connection("url", "http://example.com");
        assertEquals("url", c.getKey());
        assertEquals("http://example.com", c.getValue());
    }

    @Test
    public void testSetKeyAndGetKey() {
        Connection c = new Connection();
        c.setKey("db.url");
        assertEquals("db.url", c.getKey());
    }

    @Test
    public void testSetValueAndGetValue() {
        Connection c = new Connection();
        c.setValue("jdbc:mysql://localhost/test");
        assertEquals("jdbc:mysql://localhost/test", c.getValue());
    }

    @Test
    public void testInitializeOverridesValues() {
        Connection c = new Connection("old-key", "old-value");
        c.initialize("new-key", "new-value");
        assertEquals("new-key", c.getKey());
        assertEquals("new-value", c.getValue());
    }

    @Test
    public void testToXMLContainsKeyAndValue() throws IOException {
        Connection c = new Connection("endpoint", "gsiftp://site.edu");
        StringWriter sw = new StringWriter();
        c.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("key=\"endpoint\""));
        assertThat(xml, containsString("gsiftp://site.edu"));
        assertThat(xml, containsString("<connection"));
        assertThat(xml, containsString("</connection>"));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        Connection c = new Connection("myKey", "myValue");
        Connection cloned = (Connection) c.clone();
        assertNotSame(c, cloned);
        assertEquals(c.getKey(), cloned.getKey());
        assertEquals(c.getValue(), cloned.getValue());
    }

    @Test
    public void testToStringContainsConnectionElement() {
        Connection c = new Connection("host", "node.example.org");
        String str = c.toString();
        assertThat(str, containsString("<connection"));
    }
}
