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
import java.io.Writer;
import org.junit.jupiter.api.Test;

/** Tests for AbstractSiteData via a minimal concrete subclass. */
public class AbstractSiteDataTest {

    /** Minimal concrete subclass for testing AbstractSiteData methods. */
    private static class ConcreteData extends AbstractSiteData {
        private final String mContent;

        ConcreteData(String content) {
            this.mContent = content;
        }

        @Override
        public void toXML(Writer writer, String indent) throws IOException {
            writer.write(indent);
            writer.write("<data>");
            writer.write(mContent);
            writer.write("</data>");
        }

        @Override
        public void accept(SiteDataVisitor visitor) throws IOException {
            visitor.visit(this);
        }
    }

    /** Concrete subclass whose XML writer always fails. */
    private static class FailingData extends AbstractSiteData {

        @Override
        public void toXML(Writer writer, String indent) throws IOException {
            throw new IOException("boom");
        }

        @Override
        public void accept(SiteDataVisitor visitor) throws IOException {
            visitor.visit(this);
        }
    }

    private static class RecordingVisitor implements SiteDataVisitor {
        private SiteData mVisited;

        public SiteData getVisited() {
            return mVisited;
        }

        @Override
        public void initialize(Writer writer) {}

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
        public void visit(SiteData data) {
            mVisited = data;
        }

        @Override
        public void depart(SiteData data) {}
    }

    @Test
    public void testToXMLStringNoIndent() throws IOException {
        ConcreteData data = new ConcreteData("hello");
        String xml = data.toXML();
        assertThat(xml, is("<data>hello</data>"));
    }

    @Test
    public void testToStringDelegatesToToXML() {
        ConcreteData data = new ConcreteData("world");
        String str = data.toString();
        assertThat(str, containsString("<data>world</data>"));
    }

    @Test
    public void testToStringSwallowsIOExceptionAndReturnsEmptyString() {
        FailingData data = new FailingData();

        assertThat(data.toString(), is(""));
    }

    @Test
    public void testWriteAttributeFormatsCorrectly() throws IOException {
        ConcreteData data = new ConcreteData("");
        StringWriter sw = new StringWriter();
        data.writeAttribute(sw, "key", "value");
        assertThat(sw.toString(), is(" key=\"value\""));
    }

    @Test
    public void testWriteAttributeMultipleAttributes() throws IOException {
        ConcreteData data = new ConcreteData("");
        StringWriter sw = new StringWriter();
        data.writeAttribute(sw, "name", "local");
        data.writeAttribute(sw, "arch", "x86_64");
        String result = sw.toString();
        assertThat(result, containsString(" name=\"local\""));
        assertThat(result, containsString(" arch=\"x86_64\""));
    }

    @Test
    public void testCloneProducesDistinctInstance() throws CloneNotSupportedException {
        ConcreteData data = new ConcreteData("clone-test");
        ConcreteData cloned = (ConcreteData) data.clone();
        assertThat(cloned, is(notNullValue()));
        assertNotSame(data, cloned);
        assertThat(cloned.toString(), is(data.toString()));
    }

    @Test
    public void testToXMLWithIndent() throws IOException {
        ConcreteData data = new ConcreteData("indented");
        StringWriter sw = new StringWriter();
        data.toXML(sw, "  ");
        assertThat(sw.toString(), startsWith("  <data>"));
    }

    @Test
    public void testAcceptDispatchesToVisitor() throws IOException {
        ConcreteData data = new ConcreteData("visited");
        RecordingVisitor visitor = new RecordingVisitor();

        data.accept(visitor);

        assertThat(visitor.getVisited(), is(data));
    }
}
