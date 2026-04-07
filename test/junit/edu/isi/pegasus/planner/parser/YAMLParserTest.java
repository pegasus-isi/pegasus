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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class YAMLParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorInitializesLoggerPropertiesAndLoaderLimits() throws Exception {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty("pegasus.parser.document.size", "7");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        TestYAMLParser parser = new TestYAMLParser(bag);

        assertThat(
                "Constructor should store PegasusProperties",
                getField(parser, "mProps"),
                sameInstance(properties));
        assertThat(
                "Constructor should store the logger", getField(parser, "mLogger"), notNullValue());
        assertThat(
                "Configured parser size should be honored",
                getIntField(parser, "mMAXParsedDocSize"),
                is(7));

        Object loaderOptions = getField(parser, "mLoaderOptions");
        int limit = ((org.yaml.snakeyaml.LoaderOptions) loaderOptions).getCodePointLimit();
        org.hamcrest.MatcherAssert.assertThat(
                "LoaderOptions code-point limit should be derived from MB size",
                limit,
                org.hamcrest.Matchers.is(7 * 1024 * 1024));
    }

    @Test
    public void testNiceString() {
        TestYAMLParser parser = new TestYAMLParser(createBag());

        assertThat("Null input should stay null", parser.niceString(null), nullValue());
        assertThat(
                "Matching surrounding quotes should be stripped",
                parser.niceString("\"x\""),
                is("x"));
        assertThat("Unbalanced quotes should be preserved", parser.niceString("x\""), is("x\""));
        assertThat("Plain strings should be unchanged", parser.niceString("ab"), is("ab"));
    }

    @Test
    public void testParseErrorReturnsExceptionToString() throws Exception {
        TestYAMLParser parser = new TestYAMLParser(createBag());
        JacksonYAMLParseException exception = getYamlParseException("key: [unterminated");

        assertThat(
                "parseError should currently delegate to the exception string",
                parser.parseError(exception),
                is(exception.toString()));
    }

    @Test
    public void testValidateThrowsScannerExceptionForInvalidYamlSyntax() throws Exception {
        TestYAMLParser parser = new TestYAMLParser(createBag());
        Path file = Files.createTempFile("invalid-yaml-", ".yml");

        try {
            Files.write(file, "key: [unterminated".getBytes(StandardCharsets.UTF_8));

            ScannerException exception =
                    assertThrows(
                            ScannerException.class,
                            () -> parser.callValidate(file.toFile(), new File("ignored"), "test"),
                            "Invalid YAML syntax should fail before schema validation");

            assertThat(
                    "ScannerException should include a parse message",
                    exception.getMessage(),
                    notNullValue());
            assertThat(
                    "ScannerException message should not be empty for invalid YAML",
                    exception.getMessage().isEmpty(),
                    is(false));
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

    private JacksonYAMLParseException getYamlParseException(String yaml) throws IOException {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.readTree(yaml);
            fail("Expected invalid YAML to throw JacksonYAMLParseException");
            return null;
        } catch (JacksonYAMLParseException e) {
            return e;
        }
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private int getIntField(Object target, String name) throws Exception {
        return ((Integer) getField(target, name)).intValue();
    }

    private static final class TestYAMLParser extends YAMLParser {
        TestYAMLParser(PegasusBag bag) {
            super(bag);
        }

        boolean callValidate(File f, File schemaFile, String catalogType) {
            return validate(f, schemaFile, catalogType);
        }
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
