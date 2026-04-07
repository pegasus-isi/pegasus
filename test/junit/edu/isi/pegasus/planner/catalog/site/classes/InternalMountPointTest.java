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
public class InternalMountPointTest {

    @Test
    public void testDefaultConstructorIsEmpty() {
        InternalMountPoint imp = new InternalMountPoint();
        assertThat(imp.isEmpty(), is(true));
    }

    @Test
    public void testSingleArgConstructorSetsOnlyMountPoint() {
        InternalMountPoint imp = new InternalMountPoint("/tmp");
        assertThat(imp.getMountPoint(), is("/tmp"));
        assertThat(imp.getTotalSize(), is(nullValue()));
        assertThat(imp.getFreeSize(), is(nullValue()));
    }

    @Test
    public void testFullConstructorSetsAllFields() {
        InternalMountPoint imp = new InternalMountPoint("/data", "100GB", "50GB");
        assertThat(imp.getMountPoint(), is("/data"));
        assertThat(imp.getTotalSize(), is("100GB"));
        assertThat(imp.getFreeSize(), is("50GB"));
    }

    @Test
    public void testIsEmptyFalseWhenMountPointSet() {
        InternalMountPoint imp = new InternalMountPoint("/scratch");
        assertThat(imp.isEmpty(), is(false));
    }

    @Test
    public void testToXMLContainsMountPoint() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/lustre");
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("mount-point=\"/lustre\""));
        assertThat(xml, containsString("<internal-mount-point"));
    }

    @Test
    public void testToXMLEmptyProducesNoOutput() throws IOException {
        InternalMountPoint imp = new InternalMountPoint();
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testToXMLWithSizesIncludesSizeAttributes() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/data", "200GB", "100GB");
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("total-size=\"200GB\""));
        assertThat(xml, containsString("free-size=\"100GB\""));
    }

    @Test
    public void testToXMLWithSingleArgConstructorOmitsNullSizeAttributes() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/data");
        StringWriter sw = new StringWriter();

        imp.toXML(sw, "");

        assertThat(sw.toString(), containsString("mount-point=\"/data\""));
        assertThat(sw.toString(), not(containsString("total-size=")));
        assertThat(sw.toString(), not(containsString("free-size=")));
    }

    @Test
    public void testToXMLRespectsIndentAndSystemNewline() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/data", "200GB", "100GB");
        StringWriter sw = new StringWriter();

        imp.toXML(sw, "  ");

        assertThat(
                sw.toString(),
                is(
                        "  <internal-mount-point mount-point=\"/data\" free-size=\"100GB\" total-size=\"200GB\"/>"
                                + System.lineSeparator()));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        InternalMountPoint imp = new InternalMountPoint("/work", "512GB", "256GB");
        InternalMountPoint cloned = (InternalMountPoint) imp.clone();
        assertNotSame(imp, cloned);
        assertThat(cloned.getMountPoint(), is(imp.getMountPoint()));
        assertThat(cloned.getTotalSize(), is(imp.getTotalSize()));
        assertThat(cloned.getFreeSize(), is(imp.getFreeSize()));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        InternalMountPoint original = new InternalMountPoint("/work", "512GB", "256GB");
        InternalMountPoint cloned = (InternalMountPoint) original.clone();

        original.setMountPoint("/changed");
        original.setTotalSize("1024GB");
        original.setFreeSize("128GB");

        assertThat(cloned.getMountPoint(), is("/work"));
        assertThat(cloned.getTotalSize(), is("512GB"));
        assertThat(cloned.getFreeSize(), is("256GB"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        InternalMountPoint imp = new InternalMountPoint("/data");

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> imp.accept(new NoOpVisitor()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

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
}
