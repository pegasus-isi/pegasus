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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the ReplicaCatalog class in site classes. */
public class ReplicaCatalogTest {

    @Test
    public void testReplicaCatalogExtendsAbstractSiteData() {
        assertThat(AbstractSiteData.class.isAssignableFrom(ReplicaCatalog.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        ReplicaCatalog rc = new ReplicaCatalog();
        assertThat(rc, is(notNullValue()));
    }

    @Test
    public void testConstructorWithUrlAndType() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertThat(rc, is(notNullValue()));
    }

    @Test
    public void testGetUrlReturnsSetValue() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertThat(rc.getURL(), is("http://example.com/rc"));
    }

    @Test
    public void testGetTypeReturnsSetValue() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertThat(rc.getType(), is("File"));
    }

    @Test
    public void testSettersOverrideUrlAndType() {
        ReplicaCatalog rc = new ReplicaCatalog();

        rc.setURL("http://example.com/updated");
        rc.setType("SimpleFile");

        assertThat(rc.getURL(), is("http://example.com/updated"));
        assertThat(rc.getType(), is("SimpleFile"));
    }

    @Test
    public void testAddAliasStoresUniqueAliases() {
        ReplicaCatalog rc = new ReplicaCatalog();

        rc.addAlias("local");
        rc.addAlias("local");
        rc.addAlias("condorpool");

        List<String> aliases = aliases(rc);
        assertThat(aliases, hasSize(2));
        assertThat(aliases, hasItem("local"));
        assertThat(aliases, hasItem("condorpool"));
    }

    @Test
    public void testClearAliasesRemovesAllAliases() {
        ReplicaCatalog rc = new ReplicaCatalog();
        rc.addAlias("local");
        rc.addAlias("condorpool");

        rc.clearAliases();

        assertThat(aliases(rc).isEmpty(), is(true));
    }

    @Test
    public void testAddConnectionPreservesInsertionOrder() {
        ReplicaCatalog rc = new ReplicaCatalog();
        Connection first = new Connection("user", "alice");
        Connection second = new Connection("password", "secret");

        rc.addConnection(first);
        rc.addConnection(second);

        List<Connection> connections = connections(rc);
        assertThat(connections, hasSize(2));
        assertThat(connections.get(0), is(sameInstance(first)));
        assertThat(connections.get(1), is(sameInstance(second)));
    }

    @Test
    public void testToXMLWithoutAliasesOrConnectionsIsSelfClosing() throws IOException {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        StringWriter sw = new StringWriter();

        rc.toXML(sw, "");

        assertThat(
                sw.toString(),
                is(
                        "<replica-catalog  type=\"File\" url=\"http://example.com/rc\"/>"
                                + System.lineSeparator()));
    }

    @Test
    public void testToXMLIncludesAliasesAndConnections() throws IOException {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        rc.addAlias("local");
        rc.addConnection(new Connection("user", "alice"));
        StringWriter sw = new StringWriter();

        rc.toXML(sw, "");

        assertThat(sw.toString(), containsString("<alias "));
        assertThat(sw.toString(), containsString("name=\"local\""));
        assertThat(sw.toString(), containsString("<connection "));
        assertThat(sw.toString(), containsString("key=\"user\""));
    }

    @Test
    public void testAcceptVisitsAndDepartsReplicaCatalogWhenNoConnectionsExist()
            throws IOException {
        ReplicaCatalog rc = new ReplicaCatalog();
        RecordingVisitor visitor = new RecordingVisitor();

        rc.accept(visitor);

        assertThat(visitor.events, hasSize(2));
        assertThat(visitor.events.get(0), is("visit:ReplicaCatalog"));
        assertThat(visitor.events.get(1), is("depart:ReplicaCatalog"));
    }

    @Test
    public void testAcceptWithConnectionsThrowsUnsupportedOperationExceptionFromConnection()
            throws IOException {
        ReplicaCatalog rc = new ReplicaCatalog();
        rc.addConnection(new Connection("user", "alice"));

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> rc.accept(new RecordingVisitor()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

    @Test
    public void testCloneCopiesAliasesAndConnectionsIntoNewCollections() {
        ReplicaCatalog rc = new ReplicaCatalog();
        rc.addAlias("local");
        rc.addConnection(new Connection("user", "alice"));

        ReplicaCatalog cloned = (ReplicaCatalog) rc.clone();

        assertNotSame(rc, cloned);
        assertNotSame(connections(rc).get(0), connections(cloned).get(0));

        rc.addAlias("condorpool");
        rc.addConnection(new Connection("password", "secret"));

        assertThat(aliases(rc), hasSize(2));
        assertThat(aliases(cloned), hasSize(1));
        assertThat(connections(rc), hasSize(2));
        assertThat(connections(cloned), hasSize(1));
    }

    private static List<String> aliases(ReplicaCatalog rc) {
        List<String> result = new ArrayList<String>();
        for (Iterator<String> it = rc.getAliasIterator(); it.hasNext(); ) {
            result.add(it.next());
        }
        return result;
    }

    private static List<Connection> connections(ReplicaCatalog rc) {
        List<Connection> result = new ArrayList<Connection>();
        for (Iterator<Connection> it = rc.getConnectionIterator(); it.hasNext(); ) {
            result.add(it.next());
        }
        return result;
    }

    private static class RecordingVisitor implements SiteDataVisitor {
        private final List<String> events = new ArrayList<String>();

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
        public void visit(ReplicaCatalog catalog) {
            events.add("visit:ReplicaCatalog");
        }

        @Override
        public void depart(ReplicaCatalog catalog) {
            events.add("depart:ReplicaCatalog");
        }

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
