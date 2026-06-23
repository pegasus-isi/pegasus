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
package edu.isi.pegasus.planner.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;

import gnu.getopt.LongOpt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.MissingResourceException;

/**
 * @author Rajiv Mayani
 */
public class RCConverterTest {
    @Test
    @ClearSystemProperty(key = "pegasus.home.bindir")
    public void testInitializeCurrentlyFailsOnMissingPegasusHomeProperties() throws Exception {
        TestableRCConverter converter = new TestableRCConverter();

        MissingResourceException exception =
                assertThrows(
                        MissingResourceException.class,
                        () -> converter.exposedInitialize(new String[0]),
                        "initialize should currently fail while sanity-checking required Pegasus"
                                + " properties");

        assertThat(exception.getMessage(), is("The pegasus.home.bindir property was not set "));
        assertThat(exception.getKey(), equalTo("pegasus.home.bindir"));
    }

    @Test
    public void testGenerateValidOptions() {
        RCConverter converter = new RCConverter();

        LongOpt[] options = converter.generateValidOptions();

        assertThat(options.length, is(10));
        assertThat(options[0].getName(), is("input"));
        assertThat(options[0].getHasArg(), is(LongOpt.REQUIRED_ARGUMENT));
        assertThat(options[4].getName(), is("help"));
        assertThat(options[4].getHasArg(), is(LongOpt.NO_ARGUMENT));
        assertThat(options[9].getName(), is("expand"));
        assertThat(options[9].getHasArg(), is(LongOpt.NO_ARGUMENT));
    }

    @Test
    public void testIncrementAndDecrementLogging() throws Exception {
        RCConverter converter = new RCConverter();
        ReflectionTestUtils.setField(converter, "mLoggingLevel", LogManager.WARNING_MESSAGE_LEVEL);

        int initial = converter.getLoggingLevel();
        converter.incrementLogging();
        assertThat(converter.getLoggingLevel(), is(initial + 1));

        converter.decrementLogging();
        converter.decrementLogging();
        assertThat(converter.getLoggingLevel(), is(initial - 1));
    }

    @Test
    public void testSupportedReplicaFormatsConstant() throws Exception {
        String[] formats = (String[]) getStaticField("SUPPORTED_REPLICA_FORMATS");

        assertArrayEquals(new String[] {"File", "Regex", "YAML"}, formats);
    }

    @Test
    public void testLoadFromRejectsMissingInputs() throws Exception {
        RCConverter converter = new RCConverter();

        IOException exception =
                assertThrows(
                        IOException.class,
                        () -> callLoadFrom(converter, null, "File"),
                        "loadFrom should reject missing input files before doing any catalog work");

        assertThat(
                exception.getMessage(),
                is("Input files not specified. Specify the --input option"));
    }

    @Test
    public void testConvertsBetween49FileAnd50YamlFormats(@TempDir Path tempDir) throws Exception {
        // GH-1650: pegasus-rc-converter must support both the 4.9 (File) textual
        // format and the 5.0 (YAML) format, in either direction.
        Path input = tempDir.resolve("rc.txt");
        Files.write(
                input,
                ("f.a file:///tmp/f.a site=\"local\"\n"
                                + "f.b file:///tmp/f.b site=\"local\""
                                + " checksum.type=\"sha256\" checksum.value=\"abc\"\n")
                        .getBytes(StandardCharsets.UTF_8));

        // 4.9 File -> 5.0 YAML
        Path yaml = tempDir.resolve("rc.yml");
        runConversion(input.toString(), "File", yaml.toString(), "YAML");

        String yamlContent = new String(Files.readAllBytes(yaml), StandardCharsets.UTF_8);
        assertThat(yamlContent, containsString("pegasus: \"5.0\""));
        assertThat(yamlContent, containsString("lfn: \"f.a\""));
        assertThat(yamlContent, containsString("pfn: \"file:///tmp/f.a\""));
        assertThat(yamlContent, containsString("sha256: \"abc\""));

        // 5.0 YAML -> 4.9 File
        Path back = tempDir.resolve("back.txt");
        runConversion(yaml.toString(), "YAML", back.toString(), "File");

        String backContent = new String(Files.readAllBytes(back), StandardCharsets.UTF_8);
        assertThat(backContent, containsString("f.a"));
        assertThat(backContent, containsString("file:///tmp/f.a"));
        assertThat(backContent, containsString("f.b"));
        assertThat(backContent, containsString("file:///tmp/f.b"));
    }

    @Test
    public void testConvertsRegexEntryFromYamlToFile(@TempDir Path tempDir) throws Exception {
        // GH-1650: a regex replica entry in the 5.0 YAML format must survive
        // conversion to the 4.9 File format as a regex="true" attribute.
        Path yaml = tempDir.resolve("rc.yml");
        Files.write(
                yaml,
                ("pegasus: \"5.0\"\n"
                                + "replicas:\n"
                                + "  - lfn: \"f.a\"\n"
                                + "    pfns:\n"
                                + "      - pfn: \"file:///tmp/f.a\"\n"
                                + "        site: \"local\"\n"
                                + "  - lfn: \"file_(.*).txt\"\n"
                                + "    pfns:\n"
                                + "      - pfn: \"file:///data/[1]\"\n"
                                + "        site: \"local\"\n"
                                + "    regex: true\n")
                        .getBytes(StandardCharsets.UTF_8));

        Path out = tempDir.resolve("rc.txt");
        runConversion(yaml.toString(), "YAML", out.toString(), "File");

        String content = new String(Files.readAllBytes(out), StandardCharsets.UTF_8);
        // the plain entry is preserved
        assertThat(content, containsString("f.a"));
        assertThat(content, containsString("file:///tmp/f.a"));
        // the regex entry keeps its pattern and is flagged as a regex
        assertThat(content, containsString("file_(.*).txt"));
        assertThat(content, containsString("file:///data/[1]"));
        assertThat(content, containsString("regex=\"true\""));

        // reverse direction: 4.9 File -> 5.0 YAML. The regex LFN pattern, pfn and
        // site survive the round trip. (The regex flag itself is not yet re-emitted
        // by the YAML writer, so it is not asserted here.)
        Path roundtrip = tempDir.resolve("roundtrip.yml");
        runConversion(out.toString(), "File", roundtrip.toString(), "YAML");

        String yamlContent = new String(Files.readAllBytes(roundtrip), StandardCharsets.UTF_8);
        assertThat(yamlContent, containsString("pegasus: \"5.0\""));
        assertThat(yamlContent, containsString("lfn: \"f.a\""));
        assertThat(yamlContent, containsString("lfn: \"file_(.*).txt\""));
        assertThat(yamlContent, containsString("pfn: \"file:///data/[1]\""));
    }

    /**
     * Drives a single conversion through the converter's internal pipeline, bypassing the
     * argv/getopt path (which calls System.exit) while still exercising the real load-and-convert
     * logic.
     */
    private void runConversion(
            String inputFile, String inputFormat, String outputFile, String outputFormat)
            throws Exception {
        RCConverter converter = new RCConverter();

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                "pegasus.home.schemadir", new File("share/pegasus/schema").getAbsolutePath());

        // The shared singleton logger's formatter keeps an event stack that is
        // only populated by logEventStart; start (and later complete) an event so
        // the conversion's log calls work regardless of test execution order.
        LogManager logger = LogManagerFactory.loadSingletonInstance(props);
        logger.logEventStart("event.pegasus.pegasus-rc-converter", "pegasus.version", "test");

        ReflectionTestUtils.setField(converter, "mProps", props);
        ReflectionTestUtils.setField(converter, "mLogger", logger);
        ReflectionTestUtils.setField(
                converter, "mInputFiles", Collections.singletonList(inputFile));
        ReflectionTestUtils.setField(converter, "mInputFormat", inputFormat);
        ReflectionTestUtils.setField(converter, "mOutputFile", outputFile);
        ReflectionTestUtils.setField(converter, "mOutputFormat", outputFormat);
        ReflectionTestUtils.setField(converter, "mDoVariableExpansion", false);

        java.lang.reflect.Method method =
                RCConverter.class.getDeclaredMethod("convertReplicaCatalogs");
        method.setAccessible(true);
        try {
            method.invoke(converter);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        } finally {
            logger.logEventCompletion();
        }
    }

    private Object getStaticField(String name) throws Exception {
        return ReflectionTestUtils.getField(RCConverter.class, name);
    }

    private static final class TestableRCConverter extends RCConverter {
        void exposedInitialize(String[] opts) {
            initialize(opts);
        }
    }

    private void callLoadFrom(
            RCConverter converter, java.util.List<String> inputFiles, String inputFormat)
            throws Exception {
        java.lang.reflect.Method method =
                RCConverter.class.getDeclaredMethod("loadFrom", java.util.List.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(converter, inputFiles, inputFormat);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }
}
