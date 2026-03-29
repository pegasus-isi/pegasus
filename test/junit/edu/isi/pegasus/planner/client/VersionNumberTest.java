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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.LongOpt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the VersionNumber client class. */
public class VersionNumberTest {

    private VersionNumber mVersionNumber;

    @BeforeEach
    public void setUp() {
        mVersionNumber = new VersionNumber("pegasus-version");
    }

    @Test
    public void testConstructorSetsApplicationName() throws Exception {
        java.lang.reflect.Field f = VersionNumber.class.getDeclaredField("m_application");
        f.setAccessible(true);
        String appName = (String) f.get(mVersionNumber);
        assertEquals("pegasus-version", appName, "Application name should be set by constructor");
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
        assertEquals(8, opts.length, "generateValidOptions should return 8 options");
    }

    @Test
    public void testVersionInstanceIsNotNull() {
        assertNotNull(Version.instance(), "Version.instance() should not return null");
    }

    @Test
    public void testVersionStringIsNotEmpty() {
        assertFalse(Version.instance().toString().isEmpty(), "Version string should not be empty");
    }

    @Test
    public void testVersionNumberCanBeInstantiated() {
        VersionNumber vn = new VersionNumber("test-app");
        assertNotNull(vn, "VersionNumber should be instantiable");
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
        assertTrue(foundVersion, "Options should include a 'version' long option");
    }
}
