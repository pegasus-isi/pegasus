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

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ProfileTest {

    @Test
    public void testDefaultConstructorLeavesFieldsNull() {
        Profile p = new Profile();

        assertThat(p.getProfileNamespace(), nullValue());
        assertThat(p.getProfileKey(), nullValue());
        assertThat(p.getProfileValue(), nullValue());
    }

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
        assertThat(Profile.namespaceValid(Profile.CONDOR), is(true));
    }

    @Test
    public void testNamespaceValidGlobus() {
        assertThat(Profile.namespaceValid(Profile.GLOBUS), is(true));
    }

    @Test
    public void testNamespaceValidPegasus() {
        assertThat(Profile.namespaceValid(Profile.VDS), is(true));
    }

    @Test
    public void testNamespaceValidDagman() {
        assertThat(Profile.namespaceValid(Profile.DAGMAN), is(true));
    }

    @Test
    public void testNamespaceValidHints() {
        assertThat(Profile.namespaceValid(Profile.HINTS), is(true));
    }

    @Test
    public void testNamespaceValidEnv() {
        assertThat(Profile.namespaceValid(Profile.ENV), is(true));
    }

    @Test
    public void testNamespaceValidMetadata() {
        assertThat(Profile.namespaceValid(Profile.METADATA), is(true));
    }

    @Test
    public void testNamespaceValidSelector() {
        assertThat(Profile.namespaceValid(Profile.SELECTOR), is(true));
    }

    @Test
    public void testNamespaceValidIsCaseInsensitive() {
        assertThat(Profile.namespaceValid("ENV"), is(true));
        assertThat(Profile.namespaceValid("Env"), is(true));
        assertThat(Profile.namespaceValid("CONDOR"), is(true));
    }

    @Test
    public void testNamespaceValidReturnsFalseForNull() {
        assertThat(Profile.namespaceValid(null), is(false));
    }

    @Test
    public void testNamespaceValidReturnsFalseForShortString() {
        assertThat(Profile.namespaceValid("a"), is(false));
    }

    @Test
    public void testNamespaceValidReturnsFalseForUnknownNamespace() {
        assertThat(Profile.namespaceValid("unknown"), is(false));
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
    public void testSetProfileSetsNamespaceKeyAndValue() {
        Profile p = new Profile();

        p.setProfile(Profile.CONDOR, "requirements", "memory > 1024");

        assertThat(p.getProfileNamespace(), is(Profile.CONDOR));
        assertThat(p.getProfileKey(), is("requirements"));
        assertThat(p.getProfileValue(), is("memory > 1024"));
    }

    @Test
    public void testSetProfileWithInvalidNamespaceThrowsException() {
        Profile p = new Profile();

        assertThrows(RuntimeException.class, () -> p.setProfile("bad", "k", "v"));
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
    public void testToMultiLineDelegatesToToString() {
        Profile p = new Profile(Profile.ENV, "HOME", "/root");

        assertThat(p.toMultiLine(), is(p.toString()));
    }

    @Test
    public void testToXMLReturnsExpectedFormat() {
        Profile p = new Profile(Profile.ENV, "HOME", "/root");

        assertThat(p.toXML(), is("<profile namespace=\"env\" key=\"HOME\" >/root</profile>"));
    }

    @Test
    public void testUnknownNamespaceMessageContainsNamespaceAndValidTypes() {
        String message = Profile.unknownNamespaceMessage("bad");

        assertThat(message, containsString("bad"));
        assertThat(message, containsString(Profile.validTypesToString()));
    }

    @Test
    public void testEqualsForSameNamespaceKeyValue() {
        Profile p1 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        Profile p2 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        assertThat(p1, is(p2));
    }

    @Test
    public void testEqualsDifferentValueNotEqual() {
        Profile p1 = new Profile(Profile.ENV, "PATH", "/usr/bin");
        Profile p2 = new Profile(Profile.ENV, "PATH", "/opt/bin");
        assertThat(p1, not(equalTo(p2)));
    }

    @Test
    public void testEqualsDifferentNamespaceNotEqual() {
        Profile p1 = new Profile(Profile.ENV, "key", "val");
        Profile p2 = new Profile(Profile.CONDOR, "key", "val");
        assertThat(p1, not(equalTo(p2)));
    }

    @Test
    public void testEqualsNullReturnsFalse() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        assertThat(p.equals(null), is(false));
    }

    @Test
    public void testEqualsNonProfileReturnsFalse() {
        Profile p = new Profile(Profile.ENV, "k", "v");
        assertThat(p.equals("not a profile"), is(false));
    }

    @Test
    public void testCloneProducesEqualObject() {
        Profile original = new Profile(Profile.GLOBUS, "maxtime", "60");
        Profile clone = (Profile) original.clone();
        assertThat(clone, is(original));
    }

    @Test
    public void testCloneIsIndependentObject() {
        Profile original = new Profile(Profile.GLOBUS, "maxtime", "60");
        Profile clone = (Profile) original.clone();
        assertThat(clone, not(sameInstance(original)));
    }

    @Test
    public void testGetProfileReturnsList() {
        Profile p = new Profile(Profile.ENV, "key", "val");
        java.util.List list = p.getProfile();
        assertThat(list.size(), is(3));
        assertThat(list.get(0), is(Profile.ENV));
        assertThat(list.get(1), is("key"));
        assertThat(list.get(2), is("val"));
    }

    @Test
    public void testEqualsReturnsTrueForDefaultProfilesWithNullFields() {
        assertThat(new Profile(), is(new Profile()));
    }

    @Test
    public void testCloneOfDefaultProfileReturnsNull() {
        Profile profile = new Profile();

        assertThat(profile.clone(), nullValue());
    }
}
