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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for the XML4PrintVisitor class. */
public class XML4PrintVisitorTest {

    @Test
    public void testXML4PrintVisitorExtendsAbstractXMLPrintVisitor() {
        assertThat(
                AbstractXMLPrintVisitor.class.isAssignableFrom(XML4PrintVisitor.class), is(true));
    }

    @Test
    public void testSchemaNamespaceConstant() {
        assertThat(XML4PrintVisitor.SCHEMA_NAMESPACE, is(notNullValue()));
        assertThat(XML4PrintVisitor.SCHEMA_NAMESPACE.contains("pegasus.isi.edu"), is(true));
    }

    @Test
    public void testSchemaLocationConstant() {
        assertThat(XML4PrintVisitor.SCHEMA_LOCATION, is(notNullValue()));
        assertThat(XML4PrintVisitor.SCHEMA_LOCATION.endsWith(".xsd"), is(true));
    }

    @Test
    public void testSchemaVersionConstant() {
        assertThat(XML4PrintVisitor.SCHEMA_VERSION, is("4.0"));
    }

    @Test
    public void testEmptySiteStoreSerializationWritesSchemaFourHeaderAndFooter()
            throws IOException {
        XML4PrintVisitor visitor = new XML4PrintVisitor();
        StringWriter writer = new StringWriter();
        SiteStore store = new SiteStore();

        visitor.initialize(writer);
        store.accept(visitor);

        assertThat(writer.toString(), containsString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assertThat(writer.toString(), containsString("<sitecatalog xmlns=\""));
        assertThat(writer.toString(), containsString("version=\"4.0\""));
        assertThat(writer.toString(), containsString("</sitecatalog>"));
    }

    @Test
    public void testSiteSerializationRendersSchemaFourDirectoriesAndReplicaCatalog()
            throws IOException {
        XML4PrintVisitor visitor = new XML4PrintVisitor();
        StringWriter writer = new StringWriter();
        SiteStore store = new SiteStore();
        SiteCatalogEntry site = new SiteCatalogEntry("condorpool");
        site.addDirectory(createDirectory(Directory.TYPE.shared_scratch, "/shared/scratch"));
        site.addDirectory(createDirectory(Directory.TYPE.local_storage, "/local/storage"));
        ReplicaCatalog catalog = new ReplicaCatalog("file:///tmp/rc.txt", "SimpleFile");
        catalog.addAlias("local");
        site.addReplicaCatalog(catalog);
        store.addEntry(site);

        visitor.initialize(writer);
        store.accept(visitor);

        String xml = writer.toString();
        assertThat(xml, containsString("<site  handle=\"condorpool\""));
        assertThat(
                xml,
                containsString("<directory  path=\"/shared/scratch\" type=\"shared-scratch\""));
        assertThat(
                xml, containsString("<directory  path=\"/local/storage\" type=\"local-storage\""));
        assertThat(
                xml,
                containsString("<file-server  operation=\"all\" url=\"file:///shared/scratch\">"));
        assertThat(
                xml,
                containsString("<file-server  operation=\"all\" url=\"file:///local/storage\">"));
        assertThat(
                xml,
                containsString(
                        "<replica-catalog  type=\"SimpleFile\" url=\"file:///tmp/rc.txt\">"));
        assertThat(xml, containsString("<alias  name=\"local\"/>"));
    }

    private static Directory createDirectory(Directory.TYPE type, String mountPoint) {
        Directory directory = new Directory();
        directory.setType(type);
        directory.setInternalMountPoint(new InternalMountPoint(mountPoint));
        FileServer server = new FileServer("file", "file://", mountPoint);
        server.setSupportedOperation(FileServer.OPERATION.put);
        directory.addFileServer(server);
        return directory;
    }
}
