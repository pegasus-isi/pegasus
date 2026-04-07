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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import gnu.getopt.LongOpt;
import java.io.IOException;
import java.util.MissingResourceException;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class RCConverterTest {
    @Test
    @ClearSystemProperty(key = "pegasus.home.bindir")
    public void testInitializeCurrentlyFailsOnMissingPegasusHomeProperties() throws Exception {
        TestableRCConverter converter = new TestableRCConverter();

        MissingResourceException exception =
                assertThrows(
                        MissingResourceException.class,
                        () -> converter.exposedInitialize(new String[0]),
                        "initialize should currently fail while sanity-checking required Pegasus properties");

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
