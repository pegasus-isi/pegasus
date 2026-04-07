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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.LongOpt;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Unit tests for the VersionNumber client class. */
public class VersionNumberTest {

    private VersionNumber mVersionNumber;

    @BeforeEach
    public void setUp() {
        mVersionNumber = new VersionNumber("pegasus-version");
    }

    @Test
    public void testConstructorSetsApplicationName() throws Exception {
        String appName = (String) ReflectionTestUtils.getField(mVersionNumber, "m_application");
        assertThat(appName, is("pegasus-version"));
    }

    @Test
    public void testShowVersionDoesNotThrow() {
        assertDoesNotThrow(
                () -> VersionNumber.showVersion(Version.instance(), false),
                "showVersion should not throw an exception");
    }

    @Test
    public void testShowVersionWithBuildDoesNotThrow() {
        assertDoesNotThrow(
                () -> VersionNumber.showVersion(Version.instance(), true),
                "showVersion with build info should not throw");
    }

    @Test
    public void testGenerateValidOptionsReturnsEightOptions() {
        LongOpt[] opts = mVersionNumber.generateValidOptions();
        assertThat(opts.length, is(8));
    }

    @Test
    public void testVersionInstanceIsNotNull() {
        assertThat(Version.instance(), notNullValue());
    }

    @Test
    public void testVersionStringIsNotEmpty() {
        assertThat(Version.instance().toString().isEmpty(), is(false));
    }

    @Test
    public void testVersionNumberCanBeInstantiated() {
        VersionNumber vn = new VersionNumber("test-app");
        assertThat(vn, notNullValue());
    }

    @Test
    public void testGenerateValidOptionsContainsVersionOption() {
        LongOpt[] opts = mVersionNumber.generateValidOptions();
        boolean foundVersion = false;
        for (LongOpt opt : opts) {
            if ("version".equals(opt.getName())) {
                foundVersion = true;
                break;
            }
        }
        assertThat(foundVersion, is(true));
    }

    @Test
    public void testGenerateValidOptionsContainsExpectedNamesAndValues() {
        LongOpt[] opts = mVersionNumber.generateValidOptions();

        assertThat(hasOption(opts, "major", 'M'), is(true));
        assertThat(hasOption(opts, "minor", 'm'), is(true));
        assertThat(hasOption(opts, "help", 'h'), is(true));
        assertThat(hasOption(opts, "verbose", 1), is(true));
        assertThat(hasOption(opts, "full", 'f'), is(true));
        assertThat(hasOption(opts, "long", 'l'), is(true));
        assertThat(hasOption(opts, "build", 'f'), is(true));
    }

    @Test
    public void testShowUsagePrintsUsageAndExitCodes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            mVersionNumber.showUsage();
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("Usage: pegasus-version [-f | -V | -M | -m]"));
        assertThat(text, containsString("Options:"));
        assertThat(text, containsString("--verbose"));
        assertThat(text, containsString("The following exit codes are produced:"));
    }

    @Test
    public void testShowVersionWithoutBuildPrintsPlainVersionLine() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            VersionNumber.showVersion(Version.instance(), false);
        } finally {
            System.setOut(original);
        }

        assertThat(out.toString(), is(Version.instance().toString() + System.lineSeparator()));
    }

    @Test
    public void testShowVersionWithBuildIncludesPlatformAndHash() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            VersionNumber.showVersion(Version.instance(), true);
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString(Version.instance().toString()));
        assertThat(text, containsString(Version.instance().determinePlatform()));
        assertThat(text, containsString(Version.instance().getGitHash()));
    }

    private boolean hasOption(LongOpt[] options, String name, int value) {
        for (LongOpt option : options) {
            if (name.equals(option.getName()) && value == option.getVal()) {
                return true;
            }
        }
        return false;
    }
}
