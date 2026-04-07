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
import gnu.getopt.LongOpt;
import java.util.MissingResourceException;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ExecutableTest {

    @Test
    public void testLookupConfPropertyParsesConfiguredValueAndDefault() {
        TestableExecutable executable = new TestableExecutable();

        assertThat(
                executable.exposedLookupConfProperty(
                        new String[] {"--conf", "/tmp/a.properties"}, 'c'),
                is("/tmp/a.properties"));
        assertThat(
                executable.exposedLookupConfProperty(new String[] {"-c", "/tmp/b.properties"}, 'c'),
                is("/tmp/b.properties"));
        assertThat(
                executable.exposedLookupConfProperty(new String[] {"-x"}, 'c'),
                is("." + java.io.File.separatorChar + Executable.DEFAULT_PROPERTIES_FILE));
    }

    @Test
    public void testGetCommandLineOptionsReturnsClone() throws Exception {
        TestableExecutable executable = new TestableExecutable();
        ReflectionTestUtils.setField(
                executable, "commandLineOpts", new String[] {"-i", "input.yml"});

        String[] clone = executable.exposedGetCommandLineOptions();
        clone[0] = "--modified";

        assertArrayEquals(
                new String[] {"-i", "input.yml"}, executable.exposedGetCommandLineOptions());
    }

    @Test
    public void testConvertExceptionFormatsCauseChainAndStackTraceModes() {
        Exception nested = new Exception("outer", new IllegalArgumentException("inner"));

        String compact = Executable.convertException(nested, LogManager.FATAL_MESSAGE_LEVEL);
        assertThat(compact, containsString("[1] java.lang.Exception: outer"));
        assertThat(compact, containsString("[2] java.lang.IllegalArgumentException: inner"));

        String trace = Executable.convertException(nested, LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(trace, containsString("java.lang.Exception: outer"));
        assertThat(trace, containsString("IllegalArgumentException: inner"));
    }

    @Test
    public void testGetGVDSVersionUsesStoredVersion() throws Exception {
        TestableExecutable executable = new TestableExecutable();
        ReflectionTestUtils.setField(executable, "mVersion", "5.2.0-dev");

        assertThat(executable.getGVDSVersion(), is("Pegasus Release Version 5.2.0-dev"));
    }

    @Test
    @ClearSystemProperty(key = "pegasus.home.bindir")
    public void testInitializeCurrentlyFailsOnMissingPegasusHomeProperties() {
        TestableExecutable executable = new TestableExecutable();

        MissingResourceException exception =
                assertThrows(
                        MissingResourceException.class,
                        () -> executable.exposedInitialize(new String[0]),
                        "initialize should currently fail while sanity-checking required Pegasus properties");

        assertThat(exception.getMessage(), is("The pegasus.home.bindir property was not set "));
        assertThat(exception.getKey(), equalTo("pegasus.home.bindir"));
    }

    private static final class TestableExecutable extends Executable {
        TestableExecutable() {
            super(null);
        }

        String exposedLookupConfProperty(String[] opts, char confChar) {
            return lookupConfProperty(opts, confChar);
        }

        String[] exposedGetCommandLineOptions() {
            return getCommandLineOptions();
        }

        void exposedInitialize(String[] opts) {
            initialize(opts);
        }

        @Override
        public void loadProperties() {}

        @Override
        public void printLongVersion() {}

        @Override
        public void printShortVersion() {}

        @Override
        public LongOpt[] generateValidOptions() {
            return new LongOpt[0];
        }
    }
}
