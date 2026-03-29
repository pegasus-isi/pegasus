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
package edu.isi.pegasus.planner.namespace;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ENVTest {

    private ENV env;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        env = new ENV();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertTrue(env.isEmpty(), "Newly constructed ENV namespace should be empty");
    }

    @Test
    public void testNamespaceName() {
        assertThat(env.namespaceName(), is(ENV.NAMESPACE_NAME));
    }

    @Test
    public void testConstructAndGet() {
        env.construct("PATH", "/usr/bin:/bin");
        assertThat((String) env.get("PATH"), is("/usr/bin:/bin"));
    }

    @Test
    public void testConstructInitializesMapLazily() {
        // Before construct, map is null
        env.construct("HOME", "/home/user");
        assertTrue(env.containsKey("HOME"), "Key should be present after construct");
    }

    @Test
    public void testCheckKeyValidForNonNull() {
        assertThat(env.checkKey("PATH", "/usr/bin"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedForNullKey() {
        assertThat(env.checkKey(null, "/usr/bin"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedForNullValue() {
        assertThat(env.checkKey("PATH", null), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testToCondorReturnsNullWhenEmpty() {
        assertNull(env.toCondor(), "toCondor on empty ENV should return null");
    }

    @Test
    public void testToCondorContainsEnvironmentPrefix() {
        env.construct("PATH", "/usr/bin");
        String result = env.toCondor();
        assertThat(result, containsString("environment = "));
    }

    @Test
    public void testToCondorContainsKeyValue() {
        env.construct("MYVAR", "myvalue");
        String result = env.toCondor();
        assertThat(result, containsString("MYVAR=myvalue"));
    }

    @Test
    public void testToCondorSemicolonSeparated() {
        env.construct("A", "1");
        env.construct("B", "2");
        String result = env.toCondor();
        assertThat(result, containsString(";"));
    }

    @Test
    public void testToStringDelegatesToToCondor() {
        env.construct("X", "y");
        assertThat(env.toString(), is(env.toCondor()));
    }

    @Test
    public void testCloneOnEmptyReturnsEmptyENV() {
        ENV clone = (ENV) env.clone();
        assertTrue(clone.isEmpty());
    }

    @Test
    public void testClonePreservesValues() {
        env.construct("HOME", "/home/test");
        ENV clone = (ENV) env.clone();
        assertThat((String) clone.get("HOME"), is("/home/test"));
    }

    @Test
    public void testCloneIsIndependent() {
        env.construct("HOME", "/home/test");
        ENV clone = (ENV) env.clone();
        clone.construct("NEW_VAR", "new_value");
        assertFalse(env.containsKey("NEW_VAR"), "Clone modification should not affect original");
    }

    @Test
    public void testMergeAddsKeysFromOtherNamespace() {
        env.construct("PATH", "/usr/bin");
        ENV other = new ENV();
        other.construct("HOME", "/home/user");
        env.merge(other);
        assertTrue(env.containsKey("HOME"));
    }

    @Test
    public void testMergeOverridesExistingKey() {
        env.construct("PATH", "/usr/bin");
        ENV other = new ENV();
        other.construct("PATH", "/opt/bin");
        env.merge(other);
        assertThat((String) env.get("PATH"), is("/opt/bin"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> env.merge(new Condor()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testCheckKeyInNSFromSemicolonString() {
        env.checkKeyInNS("PATH=/usr/bin;HOME=/home/user");
        assertTrue(env.containsKey("PATH"));
        assertTrue(env.containsKey("HOME"));
    }
}
