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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ProfileTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testConstructorSetsNamespaceKeyValue() {
        Profile p = new Profile(Profile.ENV, "PATH", "/usr/bin");
        assertThat(p.getProfileNamespace(), is(Profile.ENV));
        assertThat(p.getProfileKey(), is("PATH"));
        assertThat(p.getProfileValue(), is("/usr/bin"));
    }

    @Test
    public void testConstructorWithInvalidNamespaceThrowsException() {
        assertThrows(RuntimeException.class, () -> new Profile("invalidNamespace", "key", "value"));
    }

    @Test
    public void testNamespaceValidCondor() {
        assertTrue(Profile.namespaceValid(Profile.CONDOR));
    }

    @Test
    public void testNamespaceValidGlobus() {
        assertTrue(Profile.namespaceValid(Profile.GLOBUS));
    }

    @Test
    public void testNamespaceValidPegasus() {
        assertTrue(Profile.namespaceValid(Profile.VDS));
    }

    @Test
    public void testNamespaceValidDagman() {
        assertTrue(Profile.namespaceValid(Profile.DAGMAN));
    }

    @Test
    public void testNamespaceValidHints() {
        assertTrue(Profile.namespaceValid(Profile.HINTS));
    }

    @Test
    public void testNamespaceValidEnv() {
        assertTrue(Profile.namespaceValid(Profile.ENV));
    }

    @Test
    public void testNamespaceValidMetadata() {
        assertTrue(Profile.namespaceValid(Profile.METADATA));
    }

    @Test
    public void testNamespaceValidSelector() {
        assertTrue(Profile.namespaceValid(Profile.SELECTOR));
    }

    @Test
    public void testNamespaceValidIsCaseInsensitive() {
        assertTrue(Profile.namespaceValid("ENV"));
        assertTrue(Profile.namespaceValid("Env"));
        assertTrue(Profile.namespaceValid("CONDOR"));
    }

    @Test
    public void testNamespaceValidReturnsFalseForNull() {
        assertFalse(Profile.namespaceValid(null));
    }

    @Test
    public void testNamespaceValidReturnsFalseForShortString() {
        assertFalse(Profile.namespaceValid("a"));
    }

    @Test
    public void testNamespaceValidReturnsFalseForUnknownNamespace() {
        assertFalse(Profile.namespaceValid("unknown"));
    }

    @Test
    public void testSetAndGetProfileNamespace() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        p.setProfileNamespace(Profile.CONDOR);
        assertThat(p.getProfileNamespace(), is(Profile.CONDOR));
    }

    @Test
    public void testSetProfileNamespaceWithInvalidThrowsException() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        assertThrows(RuntimeException.class, () -> p.setProfileNamespace("bogus"));
    }

    @Test
    public void testSetAndGetProfileKey() {
        Profile p = new Profile(Profile.ENV, "oldKey", "v");
        p.setProfileKey("newKey");
        assertThat(p.getProfileKey(), is("newKey"));
    }

    @Test
    public void testSetAndGetProfileValue() {
        Profile p = new Profile(Profile.ENV, "k", "oldValue");
        p.setProfileValue("newValue");
        assertThat(p.getProfileValue(), is("newValue"));
    }

    @Test
    public void testToStringContainsNamespaceKeyAndValue() {
        Profile p = new Profile(Profile.ENV, "HOME", "/root");
        String str = p.toString();
        assertThat(str, containsString(Profile.ENV));
        assertThat(str, containsString("HOME"));
        assertThat(str, containsString("/root"));
    }

    @Test
    public void testEqualsForSameNamespaceKeyValue() {
        Profile p1 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        Profile p2 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        assertEquals(p1, p2);
    }

    @Test
    public void testEqualsDifferentValueNotEqual() {
        Profile p1 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        Profile p2 = new Profile(Profile.ENV, "PATH", "/opt/bin");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testEqualsDifferentNamespaceNotEqual() {
        Profile p1 = new Profile(Profile.ENV, "key", "val");
        Profile p2 = new Profile(Profile.CONDOR, "key", "val");
        assertNotEquals(p1, p2);
    }

    @Test
    public void testEqualsNullReturnsFalse() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        assertFalse(p.equals(null));
    }

    @Test
    public void testEqualsNonProfileReturnsFalse() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        assertFalse(p.equals("not a profile"));
    }

    @Test
    public void testCloneProducesEqualObject() {
        Profile original = new Profile(Profile.GLOBUS, "maxtime", "60");
        Profile clone = (Profile) original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void testCloneIsIndependentObject() {
        Profile original = new Profile(Profile.GLOBUS, "maxtime", "60");
        Profile clone = (Profile) original.clone();
        assertNotSame(original, clone);
    }

    @Test
    public void testGetProfileReturnsList() {
        Profile p = new Profile(Profile.ENV, "key", "val");
        java.util.List list = p.getProfile();
        assertEquals(3, list.size());
        assertThat(list.get(0), is(Profile.ENV));
        assertThat(list.get(1), is("key"));
        assertThat(list.get(2), is("val"));
    }
}
