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
package edu.isi.pegasus.planner.catalog.classes;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ProfilesTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    private DefaultTestSetup mTestSetup;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "'', 200, true, 200", // overwrite is set to true
                "100, 200, true, 200",
                "100, '', true, null",
                "'', 200, false, ''", // overwrite is set to false
                "100, 200, false, 100",
                "100, '', false, 100", // entry gets deleted only if overwrite is true
            },
            nullValues = {"null"})
    public void mergeProfiles(
            String existingValue, String toAddValue, boolean overwrite, String expected) {
        String key = "test";
        Profiles existing = new Profiles();
        existing.addProfileDirectly(Profiles.NAMESPACES.condor, key, existingValue);
        Profiles toAdd = new Profiles();
        toAdd.addProfileDirectly(Profiles.NAMESPACES.condor, key, toAddValue);
        existing.merge(toAdd, overwrite);
        Namespace n = existing.get(Profiles.NAMESPACES.condor);
        assertEquals(expected, (String) n.get(key));
    }

    @AfterEach
    public void tearDown() {}

    /*
    @Test
    public void testSomeMethod() {
        assertEquals(1, 1);
    }
    */
}
