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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for AbstractXMLPrintVisitor via a minimal concrete subclass. */
public class AbstractXMLPrintVisitorTest {

    /** Minimal concrete subclass for testing. */
    private static class ConcreteVisitor extends AbstractXMLPrintVisitor {
        @Override
        public void visit(SiteStore entry) throws IOException {}

        @Override
        public void depart(SiteStore entry) throws IOException {}

        @Override
        public void visit(SiteCatalogEntry entry) throws IOException {}

        @Override
        public void depart(SiteCatalogEntry entry) throws IOException {}

        @Override
        public void visit(GridGateway entry) throws IOException {}

        @Override
        public void depart(GridGateway entry) throws IOException {}

        @Override
        public void visit(Directory directory) throws IOException {}

        @Override
        public void depart(Directory directory) throws IOException {}

        @Override
        public void visit(FileServer server) throws IOException {}

        @Override
        public void depart(FileServer server) throws IOException {}

        @Override
        public void visit(ReplicaCatalog catalog) throws IOException {}

        @Override
        public void depart(ReplicaCatalog catalog) throws IOException {}

        @Override
        public void visit(Connection c) throws IOException {}

        @Override
        public void depart(Connection c) throws IOException {}
    }

    private ConcreteVisitor visitor;
    private StringWriter sw;

    private static class MinimalSiteData extends SiteData {}

    @BeforeEach
    public void setUp() {
        sw = new StringWriter();
        visitor = new ConcreteVisitor();
        visitor.initialize(sw);
    }

    @Test
    public void testInitialIndentIsZero() {
        assertThat(visitor.mCurrentIndentIndex, is(0));
    }

    @Test
    public void testInitializeSetsWriterAndNewline() {
        StringWriter otherWriter = new StringWriter();

        visitor.initialize(otherWriter);

        assertThat(visitor.mWriter, is(otherWriter));
        assertThat(visitor.mNewLine, is(System.getProperty("line.separator", "\r\n")));
        assertThat(visitor.mCurrentIndentIndex, is(0));
    }

    @Test
    public void testGetCurrentIndentEmptyAtZero() {
        assertThat(visitor.getCurrentIndent(), is(""));
    }

    @Test
    public void testIncrementIndentIndex() {
        visitor.incrementIndentIndex();
        assertThat(visitor.mCurrentIndentIndex, is(1));
        assertThat(visitor.getCurrentIndent(), is("\t"));
    }

    @Test
    public void testDecrementIndentIndex() {
        visitor.incrementIndentIndex();
        visitor.incrementIndentIndex();
        visitor.decrementIndentIndex();
        assertThat(visitor.mCurrentIndentIndex, is(1));
    }

    @Test
    public void testGetNextIndentAddsOneTab() {
        visitor.incrementIndentIndex();
        String next = visitor.getNextIndent();
        assertThat(next, is("\t\t"));
    }

    @Test
    public void testWriteAttributeFormatsCorrectly() throws IOException {
        visitor.writeAttribute("site", "local");
        assertThat(sw.toString(), equalTo(" site=\"local\""));
    }

    @Test
    public void testWriteAttributeToSuppliedWriterFormatsCorrectly() throws IOException {
        Writer otherWriter = new StringWriter();

        visitor.writeAttribute(otherWriter, "version", "4.0");

        assertThat(otherWriter.toString(), is(" version=\"4.0\""));
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testCloseElementWritesClosingTag() throws IOException {
        visitor.incrementIndentIndex();
        visitor.closeElement("sitecatalog");
        String result = sw.toString();
        assertThat(result, is("</sitecatalog>" + System.lineSeparator()));
        assertThat(visitor.mCurrentIndentIndex, is(0));
    }

    @Test
    public void testGetCurrentIndentMultipleTabs() {
        visitor.incrementIndentIndex();
        visitor.incrementIndentIndex();
        visitor.incrementIndentIndex();
        assertThat(visitor.getCurrentIndent(), is("\t\t\t"));
    }

    @Test
    public void testInitializeResetsIndentIndex() {
        visitor.incrementIndentIndex();
        visitor.incrementIndentIndex();

        visitor.initialize(new StringWriter());

        assertThat(visitor.mCurrentIndentIndex, is(0));
        assertThat(visitor.getCurrentIndent(), is(""));
    }

    @Test
    public void testVisitSiteDataThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> visitor.visit(new MinimalSiteData()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

    @Test
    public void testDepartSiteDataThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> visitor.depart(new MinimalSiteData()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }
}
