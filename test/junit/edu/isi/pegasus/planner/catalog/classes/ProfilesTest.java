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
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** @author Rajiv Mayani */
public class ProfilesTest {

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
        assertThat((String) n.get(key), is(expected));
    }

    @Test
    public void testDefaultConstructorCreatesEmptyProfiles() {
        Profiles p = new Profiles();
        assertThat(p.isEmpty(), is(true));
    }

    @Test
    public void testAddProfileAndGet() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "PATH", "/usr/bin:/bin"));
        Namespace env = p.get(Profiles.NAMESPACES.env);
        assertThat(env, is(notNullValue()));
        assertThat((String) env.get("PATH"), is("/usr/bin:/bin"));
    }

    @Test
    public void testAddProfileDirectly() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.pegasus, "gridstart", "none");
        Namespace ns = p.get(Profiles.NAMESPACES.pegasus);
        assertThat((String) ns.get("gridstart"), is("none"));
    }

    @Test
    public void testIsEmptyReturnsFalseAfterAddingProfile() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("env", "JAVA_HOME", "/opt/java"));
        assertThat(p.isEmpty(), is(false));
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
        assertThat(envProfiles.get(0).getProfileKey(), is("MY_VAR"));
    }

    @Test
    public void testAllNamespacesPresent() {
        Profiles p = new Profiles();
        for (Profiles.NAMESPACES ns : Profiles.NAMESPACES.values()) {
            assertThat(p.get(ns), is(notNullValue()));
        }
    }

    @Test
    public void testAddProfilesFromAnotherProfilesObject() {
        Profiles source = new Profiles();
        source.addProfile(new Profile("env", "HOME", "/home/user"));

        Profiles target = new Profiles();
        target.addProfiles(source);

        Namespace env = target.get(Profiles.NAMESPACES.env);
        assertThat((String) env.get("HOME"), is("/home/user"));
    }

    @Test
    public void testCloneProducesIndependentCopy() {
        Profiles original = new Profiles();
        original.addProfile(new Profile("env", "TEST", "value"));

        Profiles clone = (Profiles) original.clone();
        clone.addProfile(new Profile("env", "OTHER", "other_value"));

        // original should not have the new key
        Namespace originalEnv = original.get(Profiles.NAMESPACES.env);
        assertThat(originalEnv.get("OTHER"), is(nullValue()));
    }

    @Test
    public void testRemoveNamespace() {
        Profiles p = new Profiles();
        p.addProfile(new Profile("condor", "universe", "vanilla"));
        Namespace removed = p.remove(Profiles.NAMESPACES.condor);
        assertThat(removed, is(notNullValue()));
        assertThat(p.get(Profiles.NAMESPACES.condor), is(nullValue()));
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

    // -----------------------------------------------------------------------
    // NAMESPACES enum
    // -----------------------------------------------------------------------

    @Test
    public void testNAMESPACESEnumHasExactlyEightValues() {
        assertThat(Profiles.NAMESPACES.values().length, is(8));
    }

    @Test
    public void testNAMESPACESEnumContainsAllExpectedValues() {
        Set<String> names = new HashSet<>();
        for (Profiles.NAMESPACES ns : Profiles.NAMESPACES.values()) {
            names.add(ns.name());
        }
        Set<String> expected =
                new HashSet<>(
                        Arrays.asList(
                                "env",
                                "globus",
                                "condor",
                                "dagman",
                                "pegasus",
                                "hints",
                                "metadata",
                                "selector"));
        assertThat(names, is(expected));
    }

    // -----------------------------------------------------------------------
    // addProfile(NAMESPACES, String, String) enum overload
    // -----------------------------------------------------------------------

    @Test
    public void testAddProfileEnumOverload_storesValue() {
        Profiles p = new Profiles();
        p.addProfile(Profiles.NAMESPACES.pegasus, "gridstart", "kickstart");
        Namespace ns = p.get(Profiles.NAMESPACES.pegasus);
        assertThat((String) ns.get("gridstart"), is("kickstart"));
    }

    @Test
    public void testAddProfileEnumOverload_isNotEmpty() {
        Profiles p = new Profiles();
        p.addProfile(Profiles.NAMESPACES.condor, "universe", "vanilla");
        assertThat(p.isEmpty(), is(false));
    }

    // -----------------------------------------------------------------------
    // addProfileDirectly(Profile)
    // -----------------------------------------------------------------------

    @Test
    public void testAddProfileDirectly_profileObject_storesValue() {
        Profiles p = new Profiles();
        p.addProfileDirectly(new Profile("condor", "request_cpus", "4"));
        Namespace ns = p.get(Profiles.NAMESPACES.condor);
        assertThat((String) ns.get("request_cpus"), is("4"));
    }

    @Test
    public void testAddProfileDirectly_profileObject_makesNotEmpty() {
        Profiles p = new Profiles();
        p.addProfileDirectly(new Profile("env", "HOME", "/home/user"));
        assertThat(p.isEmpty(), is(false));
    }

    // -----------------------------------------------------------------------
    // addProfilesDirectly(List<Profile>)
    // -----------------------------------------------------------------------

    @Test
    public void testAddProfilesDirectly_listOverload_allProfilesStored() {
        Profiles p = new Profiles();
        List<Profile> list =
                Arrays.asList(
                        new Profile("env", "A", "1"),
                        new Profile("env", "B", "2"),
                        new Profile("condor", "universe", "vanilla"));
        p.addProfilesDirectly(list);
        assertThat(p.getProfiles(Profiles.NAMESPACES.env).size(), is(2));
        assertThat(p.getProfiles(Profiles.NAMESPACES.condor).size(), is(1));
    }

    @Test
    public void testAddProfilesDirectly_listOverload_valuesPreserved() {
        Profiles p = new Profiles();
        p.addProfilesDirectly(Arrays.asList(new Profile("pegasus", "gridstart", "none")));
        assertThat((String) p.get(Profiles.NAMESPACES.pegasus).get("gridstart"), is("none"));
    }

    // -----------------------------------------------------------------------
    // addProfilesDirectly(Profiles)
    // -----------------------------------------------------------------------

    @Test
    public void testAddProfilesDirectly_profilesOverload_copiesAllProfiles() {
        Profiles source = new Profiles();
        source.addProfileDirectly(Profiles.NAMESPACES.env, "X", "xval");
        source.addProfileDirectly(Profiles.NAMESPACES.condor, "universe", "vanilla");

        Profiles target = new Profiles();
        target.addProfilesDirectly(source);

        assertThat((String) target.get(Profiles.NAMESPACES.env).get("X"), is("xval"));
        assertThat((String) target.get(Profiles.NAMESPACES.condor).get("universe"), is("vanilla"));
    }

    @Test
    public void testAddProfilesDirectly_profilesOverload_independence() {
        Profiles source = new Profiles();
        source.addProfileDirectly(Profiles.NAMESPACES.env, "KEY", "original");

        Profiles target = new Profiles();
        target.addProfilesDirectly(source);

        // Modifying source after copy should not affect target
        source.addProfileDirectly(Profiles.NAMESPACES.env, "NEW_KEY", "new_val");
        assertThat(target.get(Profiles.NAMESPACES.env).get("NEW_KEY"), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // addProfiles(List<Profile>)
    // -----------------------------------------------------------------------

    @Test
    public void testAddProfiles_listOverload_allProfilesStored() {
        Profiles p = new Profiles();
        p.addProfiles(
                Arrays.asList(new Profile("env", "K1", "v1"), new Profile("env", "K2", "v2")));
        assertThat(p.getProfiles(Profiles.NAMESPACES.env).size(), is(2));
    }

    @Test
    public void testAddProfiles_listOverload_emptyListLeavesEmpty() {
        Profiles p = new Profiles();
        p.addProfiles(Arrays.asList());
        assertThat(p.isEmpty(), is(true));
    }

    // -----------------------------------------------------------------------
    // getProfiles(NAMESPACES) — enum overload
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfiles_enumOverload_returnsCorrectProfiles() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.dagman, "maxjobs", "10");
        List<Profile> list = p.getProfiles(Profiles.NAMESPACES.dagman);
        assertThat(list.size(), is(1));
        assertThat(list.get(0).getProfileKey(), is("MAXJOBS"));
        assertThat(list.get(0).getProfileValue(), is("10"));
        assertThat(list.get(0).getProfileNamespace(), is("dagman"));
    }

    @Test
    public void testGetProfiles_enumOverload_emptyNamespace_returnsEmptyList() {
        Profiles p = new Profiles();
        List<Profile> list = p.getProfiles(Profiles.NAMESPACES.hints);
        assertThat(list, is(notNullValue()));
        assertThat(list.isEmpty(), is(true));
    }

    @Test
    public void testGetProfiles_enumOverload_multipleProfiles_allReturned() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.condor, "request_cpus", "2");
        p.addProfileDirectly(Profiles.NAMESPACES.condor, "request_memory", "1024");
        List<Profile> list = p.getProfiles(Profiles.NAMESPACES.condor);
        assertThat(list.size(), is(2));
    }

    // -----------------------------------------------------------------------
    // getProfiles(Namespace) — Namespace-object overload
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfiles_namespaceObjectOverload_returnsProfiles() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "PATH", "/bin");
        Namespace envNs = p.get(Profiles.NAMESPACES.env);
        List<Profile> list = p.getProfiles(envNs);
        assertThat(list.size(), is(1));
        assertThat(list.get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testGetProfiles_namespaceObjectOverload_namespace_isPreserved() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "HOME", "/home/user");
        Namespace envNs = p.get(Profiles.NAMESPACES.env);
        List<Profile> list = p.getProfiles(envNs);
        assertThat(list.get(0).getProfileNamespace(), is("env"));
    }

    // -----------------------------------------------------------------------
    // getProfileKeyIterator(NAMESPACES)
    // -----------------------------------------------------------------------

    @Test
    public void testGetProfileKeyIterator_returnsExpectedKeys() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.globus, "maxwalltime", "60");
        p.addProfileDirectly(Profiles.NAMESPACES.globus, "maxmemory", "512");

        Iterator it = p.getProfileKeyIterator(Profiles.NAMESPACES.globus);
        Set<String> keys = new HashSet<>();
        while (it.hasNext()) {
            keys.add((String) it.next());
        }
        assertThat(keys, is(new HashSet<>(Arrays.asList("maxwalltime", "maxmemory"))));
    }

    @Test
    public void testGetProfileKeyIterator_emptyNamespace_hasNoNext() {
        Profiles p = new Profiles();
        Iterator it = p.getProfileKeyIterator(Profiles.NAMESPACES.selector);
        assertThat(it.hasNext(), is(false));
    }

    // -----------------------------------------------------------------------
    // set(Namespace)
    // -----------------------------------------------------------------------

    @Test
    public void testSet_replacesExistingNamespace() {
        Profiles p = new Profiles();
        ENV replacement = new ENV();
        replacement.construct("CUSTOM_KEY", "custom_val");
        p.set(replacement);

        Namespace retrieved = p.get(Profiles.NAMESPACES.env);
        assertThat((String) retrieved.get("CUSTOM_KEY"), is("custom_val"));
    }

    @Test
    public void testSet_doesNotAffectOtherNamespaces() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.condor, "universe", "vanilla");
        ENV replacement = new ENV();
        replacement.construct("PATH", "/bin");
        p.set(replacement);

        // condor namespace should still have its profile
        assertThat((String) p.get(Profiles.NAMESPACES.condor).get("universe"), is("vanilla"));
    }

    // -----------------------------------------------------------------------
    // isEmpty after remove
    // -----------------------------------------------------------------------

    @Test
    public void testIsEmpty_afterRemove_withSingleNamespacePopulated() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "KEY", "val");
        assertThat(p.isEmpty(), is(false));
        p.remove(Profiles.NAMESPACES.env);
        // After removing the only populated namespace the map no longer has env key;
        // isEmpty checks nm != null before calling isEmpty on it, so should be true now
        assertThat(p.isEmpty(), is(true));
    }

    // -----------------------------------------------------------------------
    // clone preserves values
    // -----------------------------------------------------------------------

    @Test
    public void testClone_preservesExistingValues() {
        Profiles original = new Profiles();
        original.addProfileDirectly(Profiles.NAMESPACES.env, "KEY", "value");

        Profiles clone = (Profiles) original.clone();
        assertThat((String) clone.get(Profiles.NAMESPACES.env).get("KEY"), is("value"));
    }

    @Test
    public void testClone_allNamespacesPresentInClone() {
        Profiles original = new Profiles();
        Profiles clone = (Profiles) original.clone();
        for (Profiles.NAMESPACES ns : Profiles.NAMESPACES.values()) {
            assertThat(clone.get(ns), is(notNullValue()));
        }
    }

    // -----------------------------------------------------------------------
    // merge — new key added regardless of overwrite flag
    // -----------------------------------------------------------------------

    @Test
    public void testMerge_newKeyInToAdd_addedRegardlessOfOverwriteTrue() {
        Profiles existing = new Profiles();
        existing.addProfileDirectly(Profiles.NAMESPACES.condor, "universe", "vanilla");

        Profiles toAdd = new Profiles();
        toAdd.addProfileDirectly(Profiles.NAMESPACES.condor, "request_cpus", "4");

        existing.merge(toAdd, true);
        assertThat((String) existing.get(Profiles.NAMESPACES.condor).get("request_cpus"), is("4"));
    }

    @Test
    public void testMerge_newKeyInToAdd_addedRegardlessOfOverwriteFalse() {
        Profiles existing = new Profiles();
        existing.addProfileDirectly(Profiles.NAMESPACES.condor, "universe", "vanilla");

        Profiles toAdd = new Profiles();
        toAdd.addProfileDirectly(Profiles.NAMESPACES.condor, "request_cpus", "4");

        existing.merge(toAdd, false);
        assertThat((String) existing.get(Profiles.NAMESPACES.condor).get("request_cpus"), is("4"));
    }

    @Test
    public void testMerge_acrossMultipleNamespaces() {
        Profiles existing = new Profiles();
        Profiles toAdd = new Profiles();
        toAdd.addProfileDirectly(Profiles.NAMESPACES.env, "PATH", "/bin");
        toAdd.addProfileDirectly(Profiles.NAMESPACES.condor, "universe", "vanilla");

        existing.merge(toAdd, true);
        assertThat((String) existing.get(Profiles.NAMESPACES.env).get("PATH"), is("/bin"));
        assertThat(
                (String) existing.get(Profiles.NAMESPACES.condor).get("universe"), is("vanilla"));
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Test
    public void testToString_containsProfileData() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "MY_VAR", "my_val");
        String s = p.toString();
        assertThat(s, containsString("env"));
        assertThat(s, containsString("MY_VAR"));
        assertThat(s, containsString("my_val"));
    }

    @Test
    public void testToString_emptyProfiles_returnsEmptyString() {
        Profiles p = new Profiles();
        assertThat(p.toString(), is(""));
    }

    // -----------------------------------------------------------------------
    // toString(Writer, String)
    // -----------------------------------------------------------------------

    @Test
    public void testToStringWriter_writesProfileKeyword() throws Exception {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "VAR", "val");
        StringWriter sw = new StringWriter();
        p.toString(sw, "  ");
        String result = sw.toString();
        assertThat(result, containsString("profile"));
        assertThat(result, containsString("VAR"));
        assertThat(result, containsString("val"));
    }

    @Test
    public void testToStringWriter_indentIsApplied() throws Exception {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "K", "v");
        StringWriter sw = new StringWriter();
        p.toString(sw, "---");
        assertThat(sw.toString(), containsString("---"));
    }

    // -----------------------------------------------------------------------
    // toXML(Writer, String)
    // -----------------------------------------------------------------------

    @Test
    public void testToXMLWriter_containsXMLProfileElement() throws Exception {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "PATH", "/usr/bin");
        StringWriter sw = new StringWriter();
        p.toXML(sw, "  ");
        String result = sw.toString();
        assertThat(result, containsString("<profile"));
        assertThat(result, containsString("namespace=\"env\""));
        assertThat(result, containsString("key=\"PATH\""));
        assertThat(result, containsString("/usr/bin"));
    }

    @Test
    public void testToXMLWriter_indentIsApplied() throws Exception {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "K", "v");
        StringWriter sw = new StringWriter();
        p.toXML(sw, "\t\t");
        assertThat(sw.toString(), containsString("\t\t"));
    }

    // -----------------------------------------------------------------------
    // writeAttribute
    // -----------------------------------------------------------------------

    @Test
    public void testWriteAttribute_producesCorrectFormat() throws Exception {
        Profiles p = new Profiles();
        StringWriter sw = new StringWriter();
        p.writeAttribute(sw, "namespace", "env");
        assertThat(sw.toString(), is(" namespace=\"env\""));
    }

    @Test
    public void testWriteAttribute_emptyValue() throws Exception {
        Profiles p = new Profiles();
        StringWriter sw = new StringWriter();
        p.writeAttribute(sw, "key", "");
        assertThat(sw.toString(), is(" key=\"\""));
    }

    @Test
    public void testAddProfileDirectlyStringNamespaceCurrentlyThrowsNullPointerException() {
        Profiles p = new Profiles();

        assertThrows(
                NullPointerException.class, () -> p.addProfileDirectly("env", "PATH", "/usr/bin"));
    }

    @Test
    public void testAddProfileStringNamespaceCurrentlyThrowsNullPointerException() {
        Profiles p = new Profiles();

        assertThrows(NullPointerException.class, () -> p.addProfile("env", "PATH", "/usr/bin"));
    }

    @Test
    public void testRemoveReturnsNamespaceContainingExistingValues() {
        Profiles p = new Profiles();
        p.addProfileDirectly(Profiles.NAMESPACES.env, "PATH", "/usr/bin");

        Namespace removed = p.remove(Profiles.NAMESPACES.env);

        assertThat(removed, is(notNullValue()));
        assertThat((String) removed.get("PATH"), is("/usr/bin"));
    }
}
