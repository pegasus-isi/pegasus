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
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ConnectionTest {

    private static class NoOpVisitor implements SiteDataVisitor {
        @Override
        public void initialize(java.io.Writer writer) {}

        @Override
        public void visit(SiteStore entry) {}

        @Override
        public void depart(SiteStore entry) {}

        @Override
        public void visit(SiteCatalogEntry entry) {}

        @Override
        public void depart(SiteCatalogEntry entry) {}

        @Override
        public void visit(GridGateway entry) {}

        @Override
        public void depart(GridGateway entry) {}

        @Override
        public void visit(Directory directory) {}

        @Override
        public void depart(Directory directory) {}

        @Override
        public void visit(FileServer server) {}

        @Override
        public void depart(FileServer server) {}

        @Override
        public void visit(ReplicaCatalog catalog) {}

        @Override
        public void depart(ReplicaCatalog catalog) {}

        @Override
        public void visit(Connection c) {}

        @Override
        public void depart(Connection c) {}

        @Override
        public void visit(SiteData data) {}

        @Override
        public void depart(SiteData data) {}
    }

    @Test
    public void testDefaultConstructorEmptyKeyAndValue() {
        Connection c = new Connection();
        assertThat(c.getKey(), is(""));
        assertThat(c.getValue(), is(""));
    }

    @Test
    public void testOverloadedConstructorSetsKeyAndValue() {
        Connection c = new Connection("url", "http://example.com");
        assertThat(c.getKey(), is("url"));
        assertThat(c.getValue(), is("http://example.com"));
    }

    @Test
    public void testSetKeyAndGetKey() {
        Connection c = new Connection();
        c.setKey("db.url");
        assertThat(c.getKey(), is("db.url"));
    }

    @Test
    public void testSetValueAndGetValue() {
        Connection c = new Connection();
        c.setValue("jdbc:mysql://localhost/test");
        assertThat(c.getValue(), is("jdbc:mysql://localhost/test"));
    }

    @Test
    public void testInitializeOverridesValues() {
        Connection c = new Connection("old-key", "old-value");
        c.initialize("new-key", "new-value");
        assertThat(c.getKey(), is("new-key"));
        assertThat(c.getValue(), is("new-value"));
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
    public void testToXMLUsesIndentAndSystemNewline() throws IOException {
        Connection c = new Connection("endpoint", "gsiftp://site.edu");
        StringWriter sw = new StringWriter();

        c.toXML(sw, "  ");

        assertThat(
                sw.toString(),
                is(
                        "  <connection  key=\"endpoint\">gsiftp://site.edu</connection>"
                                + System.lineSeparator()));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        Connection c = new Connection("myKey", "myValue");
        Connection cloned = (Connection) c.clone();
        assertNotSame(c, cloned);
        assertThat(cloned.getKey(), is(c.getKey()));
        assertThat(cloned.getValue(), is(c.getValue()));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        Connection original = new Connection("myKey", "myValue");
        Connection cloned = (Connection) original.clone();

        original.setKey("updated-key");
        original.setValue("updated-value");

        assertThat(cloned.getKey(), is("myKey"));
        assertThat(cloned.getValue(), is("myValue"));
    }

    @Test
    public void testToStringContainsConnectionElement() {
        Connection c = new Connection("host", "node.example.org");
        String str = c.toString();
        assertThat(str, containsString("<connection"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        Connection c = new Connection("host", "node.example.org");

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> c.accept(new NoOpVisitor()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }
}
