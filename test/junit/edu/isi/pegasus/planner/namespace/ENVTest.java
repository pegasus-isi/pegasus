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

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ENVTest {

    private ENV env;

    @BeforeEach
    public void setUp() {
        env = new ENV();
    }

    @Test
    public void testDefaultConstructorIsEmpty() {
        org.hamcrest.MatcherAssert.assertThat(
                "Newly constructed ENV namespace should be empty",
                env.isEmpty(),
                org.hamcrest.Matchers.is(true));
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
        org.hamcrest.MatcherAssert.assertThat(
                "Key should be present after construct",
                env.containsKey("HOME"),
                org.hamcrest.Matchers.is(true));
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
        org.hamcrest.MatcherAssert.assertThat(
                "toCondor on empty ENV should return null",
                env.toCondor(),
                org.hamcrest.Matchers.nullValue());
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
        org.hamcrest.MatcherAssert.assertThat(clone.isEmpty(), org.hamcrest.Matchers.is(true));
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
        org.hamcrest.MatcherAssert.assertThat(
                "Clone modification should not affect original",
                env.containsKey("NEW_VAR"),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testMergeAddsKeysFromOtherNamespace() {
        env.construct("PATH", "/usr/bin");
        ENV other = new ENV();
        other.construct("HOME", "/home/user");
        env.merge(other);
        org.hamcrest.MatcherAssert.assertThat(
                env.containsKey("HOME"), org.hamcrest.Matchers.is(true));
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
        org.hamcrest.MatcherAssert.assertThat(
                env.containsKey("PATH"), org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                env.containsKey("HOME"), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testMapConstructorPreservesInitialValues() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("PATH", "/usr/bin");
        values.put("HOME", "/home/user");

        ENV constructed = new ENV(values);

        assertThat(constructed.namespaceName(), is(ENV.NAMESPACE_NAME));
        assertThat((String) constructed.get("PATH"), is("/usr/bin"));
        assertThat((String) constructed.get("HOME"), is("/home/user"));
    }

    @Test
    public void testGetReturnsNullWhenMapUninitialized() {
        org.hamcrest.MatcherAssert.assertThat(
                env.get("MISSING"), org.hamcrest.Matchers.nullValue());
    }

    @Test
    public void testToCondorUsesInsertionOrderAndTrailingNewline() {
        env.construct("A", "1");
        env.construct("B", "2");

        assertThat(env.toCondor(), is("environment = A=1;B=2;\n"));
    }

    @Test
    public void testCheckKeyInNSWithNullStringIsNoop() {
        env.checkKeyInNS((String) null);

        org.hamcrest.MatcherAssert.assertThat(env.isEmpty(), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testCheckKeyInNSWithLiteralNullStringStopsProcessing() {
        env.checkKeyInNS("null;PATH=/usr/bin");

        org.hamcrest.MatcherAssert.assertThat(env.isEmpty(), org.hamcrest.Matchers.is(true));
    }
}
