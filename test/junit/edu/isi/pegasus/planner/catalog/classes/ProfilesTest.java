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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    @Test
    public void testDefaultConstructorCreatesEmptyProfiles() {
        Profiles p = new Profiles();
        assertTrue(p.isEmpty(), "Newly constructed Profiles should be empty");
    }

    @Test
    public void testAddProfileAndGet() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "PATH", "/usr/bin:/bin"));
        Namespace env = p.get(Profiles.NAMESPACES.env);
        assertNotNull(env);
        assertEquals("/usr/bin:/bin", (String) env.get("PATH"));
    }

    @Test
    public void testAddProfileDirectly() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.pegasus, "gridstart", "none");
        Namespace ns = p.get(Profiles.NAMESPACES.pegasus);
        assertEquals("none", (String) ns.get("gridstart"));
    }

    @Test
    public void testIsEmptyReturnsFalseAfterAddingProfile() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "JAVA_HOME", "/opt/java"));
        assertFalse(p.isEmpty(), "Profiles should not be empty after adding a profile");
    }

    @Test
    public void testGetProfilesReturnsAllProfiles() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "KEY1", "val1"));
        p.addProfile(new Profile("condor", "universe", "vanilla"));
        List<Profile> list = p.getProfiles();
        assertThat(list, hasSize(2));
    }

    @Test
    public void testGetProfilesByNamespace() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "MY_VAR", "my_val"));
        p.addProfile(new Profile("pegasus", "gridstart", "kickstart"));
        List<Profile> envProfiles = p.getProfiles("env");
        assertThat(envProfiles, hasSize(1));
        assertEquals("MY_VAR", envProfiles.get(0).getProfileKey());
    }

    @Test
    public void testAllNamespacesPresent() {
        Profiles p = new Profiles();
        for (Profiles.NAMESPACES ns : Profiles.NAMESPACES.values()) {
            assertNotNull(p.get(ns), "Namespace " + ns + " should be present by default");
        }
    }

    @Test
    public void testAddProfilesFromAnotherProfilesObject() {
        Profiles source = new Profiles();
        source.addProfile(new Profile("env", "HOME", "/home/user"));

        Profiles target = new Profiles();
        target.addProfiles(source);

        Namespace env = target.get(Profiles.NAMESPACES.env);
        assertEquals("/home/user", (String) env.get("HOME"));
    }

    @Test
    public void testCloneProducesIndependentCopy() {
        Profiles original = new Profiles();
        original.addProfile(new Profile("env", "TEST", "value"));

        Profiles clone = (Profiles) original.clone();
        clone.addProfile(new Profile("env", "OTHER", "other_value"));

        // original should not have the new key
        Namespace originalEnv = original.get(Profiles.NAMESPACES.env);
        assertNull(originalEnv.get("OTHER"), "Clone modification should not affect original");
    }

    @Test
    public void testRemoveNamespace() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("condor", "universe", "vanilla"));
        Namespace removed = p.remove(Profiles.NAMESPACES.condor);
        assertNotNull(removed);
        assertNull(p.get(Profiles.NAMESPACES.condor), "Removed namespace should not be present");
    }

    @Test
    public void testToXMLIncludesProfileData() throws Exception {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "PATH", "/usr/bin"));
        String xml = p.toXML();
        assertThat(xml, containsString("namespace=\"env\""));
        assertThat(xml, containsString("key=\"PATH\""));
        assertThat(xml, containsString("/usr/bin"));
    }

    @AfterEach
    public void tearDown() {}
}
