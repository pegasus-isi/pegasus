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
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class TransformationCatalogTextParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorInitializesScannerLookAheadAndLogger() throws Exception {
        CapturingLogManager logger = new CapturingLogManager();
        TransformationCatalogTextParser parser =
                new TransformationCatalogTextParser(new StringReader("tr keg { }"), logger, false);

        assertThat(
                "Constructor should store the logger",
                getField(parser, "mLogger"),
                sameInstance(logger));
        assertThat(
                "Constructor should initialize the scanner",
                getField(parser, "mScanner"),
                notNullValue());
        assertThat(
                "Constructor should prime the look-ahead token",
                getField(parser, "mLookAhead"),
                notNullValue());
    }

    @Test
    public void testNiceString() throws Exception {
        TransformationCatalogTextParser parser =
                new TransformationCatalogTextParser(
                        new StringReader("tr keg { }"), new CapturingLogManager(), false);

        assertThat("Null input should stay null", parser.niceString(null), nullValue());
        assertThat(
                "Matching surrounding quotes should be stripped",
                parser.niceString("\"x\""),
                is("x"));
        assertThat("Unbalanced quotes should be preserved", parser.niceString("x\""), is("x\""));
    }

    @Test
    public void testParseMinimalTransformationCatalog() throws Exception {
        String text =
                "tr keg {\n"
                        + "site local {\n"
                        + "pfn \"/bin/echo\"\n"
                        + "type \"INSTALLED\"\n"
                        + "}\n"
                        + "}\n";

        TransformationCatalogTextParser parser =
                new TransformationCatalogTextParser(
                        new StringReader(text), new CapturingLogManager(), false);

        TransformationStore store = parser.parse(false);
        List<TransformationCatalogEntry> entries = store.getAllEntries();

        assertThat("One transformation entry should be parsed", entries.size(), is(1));
        assertThat(
                "Transformation name should match",
                entries.get(0).getLogicalTransformation(),
                is("keg"));
        assertThat("Site handle should match", entries.get(0).getResourceId(), is("local"));
        assertThat("PFN should match", entries.get(0).getPhysicalTransformation(), is("/bin/echo"));
        assertThat("Entry type should match", entries.get(0).getType(), is(TCType.INSTALLED));
    }

    @Test
    public void testParseRejectsUnexpectedLeadingIdentifier() throws Exception {
        TransformationCatalogTextParser parser =
                new TransformationCatalogTextParser(
                        new StringReader("identifier"), new CapturingLogManager(), false);

        ScannerException exception =
                assertThrows(
                        ScannerException.class,
                        () -> parser.parse(false),
                        "Unexpected leading identifier should be rejected");

        assertThat(
                "Error message should mention the expected reserved word",
                exception.getMessage().contains("expecting reserved word"),
                is(true));
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private static final class CapturingLogManager extends LogManager {

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
