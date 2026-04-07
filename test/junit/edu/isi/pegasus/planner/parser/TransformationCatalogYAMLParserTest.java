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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TransformationCatalogYAMLParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorSetsSchemaFile() throws Exception {
        File schemaDir = new File("share/pegasus/schema");
        TransformationCatalogYAMLParser parser =
                new TransformationCatalogYAMLParser(createBag(), schemaDir);

        Field schemaField =
                TransformationCatalogYAMLParser.class.getDeclaredField("SCHEMA_FILENAME");
        schemaField.setAccessible(true);
        File schemaFile = (File) schemaField.get(null);

        assertThat("Parser should be constructed", parser, notNullValue());
        assertThat("Constructor should initialize the schema file", schemaFile, notNullValue());
        assertThat(
                "Schema file should resolve under the yaml schema directory",
                schemaFile.getAbsolutePath(),
                is(new File(schemaDir, "yaml/tc-5.0.yml").getAbsolutePath()));
    }

    @Test
    public void testParseMissingFileReturnsEmptyStore() throws Exception {
        TransformationCatalogYAMLParser parser =
                new TransformationCatalogYAMLParser(createBag(), new File("share/pegasus/schema"));

        TransformationStore store =
                parser.parse("test/junit/does-not-exist-transformation-catalog.yml", false);

        assertThat("Parser should always return a store", store, notNullValue());
        assertThat("Missing files should result in an empty store", store.isEmpty(), is(true));
        assertThat(
                "Missing files should not create containers",
                store.getAllContainers().isEmpty(),
                is(true));
    }

    @Test
    public void testParseEmptyFileReturnsEmptyStore() throws Exception {
        TransformationCatalogYAMLParser parser =
                new TransformationCatalogYAMLParser(createBag(), new File("share/pegasus/schema"));
        Path file = Files.createTempFile("transformation-catalog-empty-", ".yml");

        try {
            Files.write(file, new byte[0]);
            TransformationStore store = parser.parse(file.toString(), false);

            assertThat("Parser should return a store for empty files", store, notNullValue());
            assertThat("Empty files should result in an empty store", store.isEmpty(), is(true));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void testParseValidMinimalCatalogReturnsEntries() throws Exception {
        TransformationCatalogYAMLParser parser =
                new TransformationCatalogYAMLParser(createBag(), new File("share/pegasus/schema"));
        Path file = Files.createTempFile("transformation-catalog-", ".yml");

        String yaml =
                "pegasus: \"5.0\"\n"
                        + "transformations:\n"
                        + "  - name: \"keg\"\n"
                        + "    sites:\n"
                        + "      - name: \"local\"\n"
                        + "        type: \"installed\"\n"
                        + "        pfn: \"/bin/echo\"\n";

        try {
            Files.write(file, yaml.getBytes(StandardCharsets.UTF_8));
            TransformationStore store = parser.parse(file.toString(), false);
            List<TransformationCatalogEntry> entries = store.getAllEntries();

            assertThat("One transformation entry should be parsed", entries.size(), is(1));
            assertThat(
                    "Transformation name should match",
                    entries.get(0).getLogicalTransformation(),
                    is("keg"));
            assertThat(
                    "Minimal catalog should not create container entries",
                    store.getAllContainers().isEmpty(),
                    is(true));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
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
