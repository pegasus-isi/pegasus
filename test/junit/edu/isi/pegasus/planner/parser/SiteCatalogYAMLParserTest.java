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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class SiteCatalogYAMLParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorInitializesSchemaAndSiteSelectionState() throws Exception {
        SiteCatalogYAMLParser parser =
                new SiteCatalogYAMLParser(createBag(), Arrays.asList("*", "local"));

        assertThat(
                "Schema URI constant should match the parser definition",
                SiteCatalogYAMLParser.SCHEMA_URI,
                is("https://pegasus.isi.edu/schema/sc-5.0.yml"));
        assertThat(
                "Presence of * should enable load-all mode",
                getBooleanField(parser, "mLoadAll"),
                is(true));

        @SuppressWarnings("unchecked")
        Set<String> sites = (Set<String>) getField(parser, "mSites");
        assertThat("Wildcard site should be tracked", sites.contains("*"), is(true));
        assertThat("Explicit site should be tracked", sites.contains("local"), is(true));

        File schemaFile = (File) getField(parser, "SCHEMA_FILENAME");
        assertThat(
                "Schema file should resolve to yaml/sc-5.0.yml",
                schemaFile.getPath().endsWith("yaml" + File.separator + "sc-5.0.yml"),
                is(true));
    }

    @Test
    public void testGetSiteStoreRequiresCompletedParsing() {
        SiteCatalogYAMLParser parser =
                new SiteCatalogYAMLParser(createBag(), Collections.singletonList("*"));

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        parser::getSiteStore,
                        "Store access should require completed parsing");
        assertThat(
                "Pre-parse guard message should match",
                exception.getMessage(),
                is("Parsing of file needs to complete before function can be called"));
    }

    @Test
    public void testStartParserMissingFileReturnsEmptyStore() {
        SiteCatalogYAMLParser parser =
                new SiteCatalogYAMLParser(createBag(), Collections.singletonList("*"));

        parser.startParser("test/junit/does-not-exist-site-catalog.yml");
        SiteStore store = parser.getSiteStore();

        assertThat(
                "Parser should initialize an empty store even for missing files",
                store,
                notNullValue());
        assertThat("Missing files should produce an empty site store", store.isEmpty(), is(true));
    }

    @Test
    public void testNiceString() {
        SiteCatalogYAMLParser parser =
                new SiteCatalogYAMLParser(createBag(), Collections.singletonList("*"));

        assertThat("Null input should stay null", parser.niceString(null), nullValue());
        assertThat(
                "Matching surrounding quotes should be stripped",
                parser.niceString("\"x\""),
                is("x"));
        assertThat("Unbalanced quotes should be preserved", parser.niceString("x\""), is("x\""));
    }

    private PegasusBag createBag() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(
                "pegasus.home.schemadir", new File("share/pegasus/schema").getAbsolutePath());
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
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
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
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
