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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.format.Simple;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class SiteCatalogXMLParser4Test {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstantsAndConstructorState() throws Exception {
        Properties connectProps = new Properties();
        connectProps.setProperty(SiteCatalog.VARIABLE_EXPANSION_KEY, "false");

        SiteCatalogXMLParser4 parser = createParser(connectProps, Arrays.asList("*", "alpha"));

        assertThat(
                "Schema location constant should match the parser definition",
                SiteCatalogXMLParser4.SCHEMA_LOCATION,
                is("https://pegasus.isi.edu/schema/sc-4.2.xsd"));
        assertThat(
                "Schema namespace constant should match the parser definition",
                SiteCatalogXMLParser4.SCHEMA_NAMESPACE,
                is("https://pegasus.isi.edu/schema/sitecatalog"));
        assertThat(
                "getSchemaNamespace should return the parser namespace constant",
                parser.getSchemaNamespace(),
                is(SiteCatalogXMLParser4.SCHEMA_NAMESPACE));
        assertThat(
                "Constructor should honor variable expansion=false",
                getBooleanField(parser, "mDoVariableExpansion"),
                is(false));
        assertThat(
                "Presence of * should enable load-all mode",
                getBooleanField(parser, "mLoadAll"),
                is(true));

        @SuppressWarnings("unchecked")
        java.util.Set<String> sites = (java.util.Set<String>) getField(parser, "mSites");
        assertThat("Wildcard site should be tracked", sites.contains("*"), is(true));
        assertThat("Explicit site should be tracked", sites.contains("alpha"), is(true));
    }

    @Test
    public void testGetSiteStoreRequiresCompletedParsing() {
        SiteCatalogXMLParser4 parser =
                createParser(new Properties(), Collections.singletonList("*"));

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        parser::getSiteStore,
                        "Parser should reject access before parsing completes");
        org.hamcrest.MatcherAssert.assertThat(
                "Pre-parse guard message should match",
                exception.getMessage(),
                org.hamcrest.Matchers.is(
                        "Parsing of file needs to complete before function can be called"));
    }

    @Test
    public void testGetSiteStoreReturnsResultAfterParsingDone() throws Exception {
        SiteCatalogXMLParser4 parser =
                createParser(new Properties(), Collections.singletonList("*"));
        SiteStore store = new SiteStore();

        setField(parser, "mResult", store);
        setField(parser, "mParsingDone", Boolean.TRUE);

        assertThat(
                "Completed parser should return the constructed store",
                parser.getSiteStore(),
                sameInstance(store));
    }

    @Test
    public void testCreateObjectBuildsDirectoryAndFileServer() {
        SiteCatalogXMLParser4 parser =
                createParser(new Properties(), Collections.singletonList("*"));
        primeLogger(parser);

        Directory directory =
                (Directory)
                        parser.createObject(
                                "directory",
                                Arrays.asList("type", "path", "free-size", "total-size"),
                                Arrays.asList("shared-scratch", "/scratch", "1000", "2000"));

        assertThat("Directory element should create a Directory object", directory, notNullValue());
        assertThat(
                "Directory type should be parsed",
                directory.getType(),
                is(Directory.TYPE.shared_scratch));
        assertThat(
                "Directory path should populate the internal mount point",
                directory.getInternalMountPoint().getMountPoint(),
                is("/scratch"));
        assertThat(
                "free-size should populate the internal mount point",
                directory.getInternalMountPoint().getFreeSize(),
                is("1000"));
        assertThat(
                "total-size should populate the internal mount point",
                directory.getInternalMountPoint().getTotalSize(),
                is("2000"));

        FileServer server =
                (FileServer)
                        parser.createObject(
                                "file-server",
                                Arrays.asList("url", "operation"),
                                Arrays.asList("gsiftp://example.com/data", "get"));

        assertThat("file-server element should create a FileServer", server, notNullValue());
        assertThat("Protocol should be derived from the URL", server.getProtocol(), is("gsiftp"));
        assertThat(
                "URL prefix should exclude the path",
                server.getURLPrefix(),
                is("gsiftp://example.com"));
        assertThat(
                "Mount point should come from the URL path", server.getMountPoint(), is("/data"));
        assertThat(
                "Operation should be parsed",
                server.getSupportedOperation(),
                is(FileServer.OPERATION.get));
    }

    @Test
    public void testSetElementRelationLoadsOnlyRequestedSites() {
        SiteCatalogXMLParser4 selectiveParser =
                createParser(new Properties(), Collections.singletonList("alpha"));
        primeLogger(selectiveParser);
        SiteStore selectiveStore = new SiteStore();
        SiteCatalogEntry alpha = new SiteCatalogEntry();
        alpha.setSiteHandle("alpha");
        SiteCatalogEntry beta = new SiteCatalogEntry();
        beta.setSiteHandle("beta");

        assertThat(
                "Matching site should still be accepted",
                selectiveParser.setElementRelation("site", selectiveStore, alpha),
                is(true));
        assertThat(
                "Non-matching site still returns true after filter evaluation",
                selectiveParser.setElementRelation("site", selectiveStore, beta),
                is(true));
        assertThat(
                "Requested site should be added to the store",
                selectiveStore.contains("alpha"),
                is(true));
        assertThat(
                "Unrequested site should be filtered out",
                selectiveStore.contains("beta"),
                is(false));

        SiteCatalogXMLParser4 wildcardParser =
                createParser(new Properties(), Collections.singletonList("*"));
        primeLogger(wildcardParser);
        SiteStore wildcardStore = new SiteStore();
        SiteCatalogEntry gamma = new SiteCatalogEntry();
        gamma.setSiteHandle("gamma");

        assertThat(
                "Wildcard parser should accept the site",
                wildcardParser.setElementRelation("site", wildcardStore, gamma),
                is(true));
        assertThat(
                "Wildcard selection should load every site",
                wildcardStore.contains("gamma"),
                is(true));
    }

    private PegasusBag createBag() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, createLogger());
        return bag;
    }

    private SiteCatalogXMLParser4 createParser(Properties connectProps, List<String> sites) {
        SiteCatalogXMLParser4 parser = new SiteCatalogXMLParser4(createBag(), connectProps, sites);
        try {
            setField(parser, "mLogger", createLogger());
        } catch (Exception e) {
            throw new RuntimeException("Unable to inject logger into parser", e);
        }
        return parser;
    }

    private NoOpLogManager createLogger() {
        NoOpLogManager logger = new NoOpLogManager();
        logger.initialize(new Simple(), new Properties());
        return logger;
    }

    private void primeLogger(SiteCatalogXMLParser4 parser) {
        try {
            Object logger = getInheritedField(parser, "mLogger");
            if (logger instanceof LogManager) {
                ((LogManager) logger).logEventStart("test.parser", "sitecatalog", "test");
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to prime parser logger", e);
        }
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private Object getInheritedField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
    }

    private void setField(Object target, String name, Object value) throws Exception {
        ReflectionTestUtils.setField(target, name, value);
    }

    private static final class NoOpLogManager extends LogManager {

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, java.io.PrintStream ps) {}

        @Override
        public java.io.PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, int level) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
