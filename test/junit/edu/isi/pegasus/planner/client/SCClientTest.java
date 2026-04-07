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
public class SCClientTest {
    @Test
    @ClearSystemProperty(key = "pegasus.home.bindir")
    public void testInitializeCurrentlyFailsOnMissingPegasusHomeProperties() throws Exception {
        TestableSCClient client = new TestableSCClient();

        MissingResourceException exception =
                assertThrows(
                        MissingResourceException.class,
                        () -> client.exposedInitialize(new String[0]),
                        "initialize should currently fail while sanity-checking required Pegasus properties");

        assertThat(exception.getMessage(), is("The pegasus.home.bindir property was not set "));
        assertThat(exception.getKey(), equalTo("pegasus.home.bindir"));
    }

    @Test
    public void testGenerateValidOptions() {
        SCClient client = new SCClient();

        LongOpt[] options = client.generateValidOptions();

        assertThat(options.length, is(9));
        assertThat(options[0].getName(), is("input"));
        assertThat(options[0].getHasArg(), is(LongOpt.REQUIRED_ARGUMENT));
        assertThat(options[3].getName(), is("help"));
        assertThat(options[3].getHasArg(), is(LongOpt.NO_ARGUMENT));
        assertThat(options[8].getName(), is("expand"));
        assertThat(options[8].getHasArg(), is(LongOpt.NO_ARGUMENT));
    }

    @Test
    public void testIncrementAndDecrementLogging() throws Exception {
        SCClient client = new SCClient();
        ReflectionTestUtils.setField(client, "mLoggingLevel", LogManager.WARNING_MESSAGE_LEVEL);

        int initial = client.getLoggingLevel();
        client.incrementLogging();
        assertThat(client.getLoggingLevel(), is(initial + 1));

        client.decrementLogging();
        client.decrementLogging();
        assertThat(client.getLoggingLevel(), is(initial - 1));
    }

    @Test
    public void testParseInputFilesRejectsMissingInputs() {
        SCClient client = new SCClient();

        IOException exception =
                assertThrows(
                        IOException.class,
                        () -> client.parseInputFiles(null, "XML", "YAML"),
                        "parseInputFiles should reject missing input files before doing any parser work");

        assertThat(
                exception.getMessage(),
                is("Input files not specified. Specify the --input option"));
    }

    private static final class TestableSCClient extends SCClient {
        void exposedInitialize(String[] opts) {
            initialize(opts);
        }
    }
}
