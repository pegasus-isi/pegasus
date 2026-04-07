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
import java.util.MissingResourceException;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class TCConverterTest {

    @Test
    @ClearSystemProperty(key = "pegasus.home.bindir")
    public void testInitializeCurrentlyFailsOnMissingPegasusHomeProperties() throws Exception {
        TestableTCConverter converter = new TestableTCConverter();

        assertThat(getStaticField("YAML_FORMAT"), is("YAML"));
        assertThat(getStaticField("TEXT_FORMAT"), is("Text"));

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
        TCConverter converter = new TCConverter();

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
        TestableTCConverter converter = new TestableTCConverter();
        ReflectionTestUtils.setField(converter, "mLoggingLevel", LogManager.WARNING_MESSAGE_LEVEL);

        int initial = converter.getLoggingLevel();
        converter.incrementLogging();
        assertThat(converter.getLoggingLevel(), is(initial + 1));

        converter.decrementLogging();
        converter.decrementLogging();
        assertThat(converter.getLoggingLevel(), is(initial - 1));
    }

    @Test
    public void testSupportedTransformationFormatsConstant() throws Exception {
        String[] formats = (String[]) getStaticField("SUPPORTED_TRANSFORMATION_FORMAT");

        assertArrayEquals(new String[] {"Text", "YAML"}, formats);
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private void setField(Object target, String name, Object value) throws Exception {
        ReflectionTestUtils.setField(target, name, value);
    }

    private Object getStaticField(String name) throws Exception {
        return ReflectionTestUtils.getField(TCConverter.class, name);
    }

    private static final class TestableTCConverter extends TCConverter {
        void exposedInitialize(String[] opts) {
            initialize(opts);
        }
    }
}
