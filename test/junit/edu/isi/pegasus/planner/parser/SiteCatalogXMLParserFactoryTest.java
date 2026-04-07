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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteCatalogXMLParserFactoryTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstants() {
        assertThat(
                "Default parser package should match the factory package",
                SiteCatalogXMLParserFactory.DEFAULT_PARSER_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.parser"));
        assertThat(
                "Default site catalog parser should be the v4 parser",
                SiteCatalogXMLParserFactory.DEFAULT_SC_PARSER_CLASS,
                is("SiteCatalogXMLParser4"));
        assertThat(
                "Schema version constants should be ordered numerically",
                SiteCatalogXMLParserFactory.SC_VERSION_4_0_0
                        > SiteCatalogXMLParserFactory.SC_VERSION_3_0_0,
                is(true));
    }

    @Test
    public void testGetMetadataReturnsRootAttributes() throws IOException {
        Path file = Files.createTempFile("site-catalog-factory-", ".xml");
        Files.write(
                file,
                Collections.singletonList(
                        "<sitecatalog xmlns=\"https://pegasus.isi.edu/schema/sitecatalog\" version=\"4.2\" schemaLocation=\"https://pegasus.isi.edu/schema sc-4.2.xsd\"/>"),
                StandardCharsets.UTF_8);

        try {
            Map metadata = SiteCatalogXMLParserFactory.getMetadata(createBag(), file.toString());

            assertThat(
                    "Version should be extracted from the root element",
                    metadata.get("version"),
                    is("4.2"));
            assertThat(
                    "schemaLocation should be extracted from the root element",
                    metadata.get("schemaLocation"),
                    is("https://pegasus.isi.edu/schema sc-4.2.xsd"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testPrivateLoadSiteCatalogParserLoadsShortAndFqcnNames() throws Exception {
        Method method =
                SiteCatalogXMLParserFactory.class.getDeclaredMethod(
                        "loadSiteCatalogParser",
                        String.class,
                        PegasusBag.class,
                        Properties.class,
                        java.util.List.class);
        method.setAccessible(true);

        Object shortNameResult =
                method.invoke(
                        null,
                        "SiteCatalogXMLParser4",
                        createBag(),
                        new Properties(),
                        Collections.singletonList("*"));
        Object fqcnResult =
                method.invoke(
                        null,
                        "edu.isi.pegasus.planner.parser.SiteCatalogXMLParser4",
                        createBag(),
                        new Properties(),
                        Collections.singletonList("*"));

        assertThat(
                "Short class name should load from the default parser package",
                shortNameResult,
                instanceOf(SiteCatalogXMLParser4.class));
        assertThat(
                "Fully qualified parser name should also load",
                fqcnResult,
                instanceOf(SiteCatalogXMLParser4.class));
    }

    @Test
    public void testLoadSiteCatalogXMLParserRejectsMissingPropertiesAndLogger() throws IOException {
        Path file = writeSiteCatalog("4.2");
        try {
            PegasusBag missingProperties = new PegasusBag();
            missingProperties.add(PegasusBag.PEGASUS_LOGMANAGER, createLogger());

            RuntimeException propertiesException =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    SiteCatalogXMLParserFactory.loadSiteCatalogXMLParser(
                                            missingProperties,
                                            new Properties(),
                                            file.toString(),
                                            Collections.singletonList("*")),
                            "Factory should reject a bag without PegasusProperties");
            assertThat(
                    "Missing properties guard message should match",
                    propertiesException.getMessage(),
                    is("Invalid properties passed"));

            PegasusBag missingLogger = new PegasusBag();
            missingLogger.add(
                    PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());

            RuntimeException loggerException =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    SiteCatalogXMLParserFactory.loadSiteCatalogXMLParser(
                                            missingLogger,
                                            new Properties(),
                                            file.toString(),
                                            Collections.singletonList("*")),
                            "Factory should reject a bag without a logger");
            assertThat(
                    "Missing logger guard message should match",
                    loggerException.getMessage(),
                    is("Invalid logger passed"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testLoadSiteCatalogXMLParserRejectsUnsupportedVersion3() throws IOException {
        Path file = writeSiteCatalog("3.0");

        try {
            SiteCatalogXMLParserFactoryException exception =
                    assertThrows(
                            SiteCatalogXMLParserFactoryException.class,
                            () ->
                                    SiteCatalogXMLParserFactory.loadSiteCatalogXMLParser(
                                            createBag(),
                                            new Properties(),
                                            file.toString(),
                                            Collections.singletonList("*")),
                            "Version 3 catalogs should be rejected");

            assertThat(
                    "Unsupported-version message should match the current behavior",
                    exception.getMessage(),
                    is("Site Catalog Schema Version 2 and Version 3 are no longer supported"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testLoadSiteCatalogXMLParserReturnsDefaultV4ParserForSupportedVersion()
            throws IOException {
        Path file = writeSiteCatalog("4.2");

        try {
            SiteCatalogXMLParser parser =
                    SiteCatalogXMLParserFactory.loadSiteCatalogXMLParser(
                            createBag(),
                            new Properties(),
                            file.toString(),
                            Collections.singletonList("*"));

            assertThat(
                    "Supported v4 catalogs should load the default v4 parser",
                    parser,
                    instanceOf(SiteCatalogXMLParser4.class));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private Path writeSiteCatalog(String version) throws IOException {
        Path file = Files.createTempFile("site-catalog-factory-", ".xml");
        Files.write(
                file,
                Collections.singletonList(
                        "<sitecatalog xmlns=\"https://pegasus.isi.edu/schema/sitecatalog\" version=\""
                                + version
                                + "\" schemaLocation=\"https://pegasus.isi.edu/schema sc-"
                                + version
                                + ".xsd\"/>"),
                StandardCharsets.UTF_8);
        return file;
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, createLogger());
        return bag;
    }

    private LogManager createLogger() {
        NoOpLogManager logger = new NoOpLogManager();
        logger.initialize(null, new Properties());
        return logger;
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
