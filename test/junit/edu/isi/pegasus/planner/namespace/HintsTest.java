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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class HintsTest {

    private Hints hints;

    @BeforeEach
    public void setUp() {
        hints = new Hints();
    }

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertThat(hints.isEmpty(), is(true));
    }

    @Test
    public void testNamespaceName() {
        assertThat(hints.namespaceName(), is(Hints.NAMESPACE_NAME));
    }

    @Test
    public void testConstructAndGet() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        assertThat((String) hints.get(Hints.EXECUTION_SITE_KEY), is("condorpool"));
    }

    @Test
    public void testConstructInitializesMapLazily() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "local");
        assertThat(hints.containsKey(Hints.EXECUTION_SITE_KEY), is(true));
    }

    @Test
    public void testCheckKeyValidExecutionSite() {
        assertThat(hints.checkKey(Hints.EXECUTION_SITE_KEY, "condorpool"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidGridJobType() {
        assertThat(hints.checkKey(Hints.GRID_JOB_TYPE_KEY, "compute"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidPfnHint() {
        assertThat(hints.checkKey(Hints.PFN_HINT_KEY, "/path/to/exec"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyDeprecatedExecutionPool() {
        assertThat(
                hints.checkKey(Hints.DEPRECATED_EXECUTION_SITE_KEY, "condorpool"),
                is(Namespace.DEPRECATED_KEY));
    }

    @Test
    public void testCheckKeyDeprecatedPfnHint() {
        assertThat(
                hints.checkKey(Hints.DEPRECATED_PFN_HINT_KEY, "/path"),
                is(Namespace.DEPRECATED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedUnknown() {
        assertThat(hints.checkKey("some_random_key", "value"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyShortKeyResultsInNotPermitted() {
        // A 1-char key sets res=MALFORMED_KEY but the switch continues on 'e' and ends
        // in NOT_PERMITTED_KEY since "e" does not match any known hint key exactly
        assertThat(hints.checkKey("e", "val"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNullValueResultsInNotPermitted() {
        // Null value triggers the malformed initial check, but the switch on 'e' continues
        // and ends in NOT_PERMITTED_KEY since "e" does not exactly match any known hint key
        assertThat(hints.checkKey("e", null), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testToCondorEmptyReturnsEmptyString() {
        assertThat(hints.toCondor(), is(""));
    }

    @Test
    public void testToCondorContainsKeyValue() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        String result = hints.toCondor();
        assertThat(result, containsString(Hints.EXECUTION_SITE_KEY));
        assertThat(result, containsString("condorpool"));
    }

    @Test
    public void testToCondorFormat() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "local");
        String result = hints.toCondor();
        assertThat(result, containsString(" = "));
    }

    @Test
    public void testToStringDelegatesToToCondor() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "local");
        assertThat(hints.toString(), is(hints.toCondor()));
    }

    @Test
    public void testCloneOnEmptyReturnsEmptyHints() {
        Hints clone = (Hints) hints.clone();
        assertThat(clone.isEmpty(), is(true));
    }

    @Test
    public void testClonePreservesValues() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        Hints clone = (Hints) hints.clone();
        assertThat((String) clone.get(Hints.EXECUTION_SITE_KEY), is("condorpool"));
    }

    @Test
    public void testCloneIsIndependent() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        Hints clone = (Hints) hints.clone();
        clone.construct(Hints.PFN_HINT_KEY, "/new/path");
        assertThat(hints.containsKey(Hints.PFN_HINT_KEY), is(false));
    }

    @Test
    public void testMergeAddsKeysFromOtherNamespace() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        Hints other = new Hints();
        other.construct(Hints.PFN_HINT_KEY, "/path/to/exec");
        hints.merge(other);
        assertThat(hints.containsKey(Hints.PFN_HINT_KEY), is(true));
    }

    @Test
    public void testMergeOverridesExistingKey() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        Hints other = new Hints();
        other.construct(Hints.EXECUTION_SITE_KEY, "local");
        hints.merge(other);
        assertThat((String) hints.get(Hints.EXECUTION_SITE_KEY), is("local"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> hints.merge(new ENV()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testDeprecatedTableContainsMappings() {
        assertThat(
                hints.deprecatedTable().containsKey(Hints.DEPRECATED_EXECUTION_SITE_KEY), is(true));
        assertThat(hints.deprecatedTable().containsKey(Hints.DEPRECATED_PFN_HINT_KEY), is(true));
    }

    @Test
    public void testMapConstructorPreservesInitialValues() {
        Map<String, String> values = new TreeMap<String, String>();
        values.put(Hints.EXECUTION_SITE_KEY, "condorpool");
        values.put(Hints.PFN_HINT_KEY, "/bin/task");

        Hints constructed = new Hints(values);

        assertThat((String) constructed.get(Hints.EXECUTION_SITE_KEY), is("condorpool"));
        assertThat((String) constructed.get(Hints.PFN_HINT_KEY), is("/bin/task"));
    }

    @Test
    public void testGetProfileKeyIteratorOnEmptyNamespaceHasNoElements() {
        Iterator iterator = hints.getProfileKeyIterator();

        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testContainsKeyReturnsFalseWhenMapUninitialized() {
        assertThat(hints.containsKey(Hints.EXECUTION_SITE_KEY), is(false));
    }

    @Test
    public void testToCondorUsesExpectedMultiLineFormatting() {
        hints.construct(Hints.EXECUTION_SITE_KEY, "condorpool");
        hints.construct(Hints.PFN_HINT_KEY, "/bin/task");

        assertThat(hints.toCondor(), is("execution.site = condorpool\npfn = /bin/task\n"));
    }

    @Test
    public void testDeprecatedTableMapsDeprecatedKeysToReplacements() {
        assertThat(
                (String) hints.deprecatedTable().get(Hints.DEPRECATED_EXECUTION_SITE_KEY),
                is(Hints.EXECUTION_SITE_KEY));
        assertThat(
                (String) hints.deprecatedTable().get(Hints.DEPRECATED_PFN_HINT_KEY),
                is(Hints.PFN_HINT_KEY));
    }
}
