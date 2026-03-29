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
public class DagmanTest {

    private Dagman dagman;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        dagman = new Dagman();
        dagman.setJobName("test_job");
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructor() {
        Dagman d = new Dagman();
        assertThat(d.namespaceName(), is(Dagman.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresUpperCaseKey() {
        dagman.construct("retry", "3");
        assertTrue(dagman.containsKey("RETRY"), "Keys should be stored in UPPER CASE");
        assertFalse(dagman.containsKey("retry"), "Lower case key should not be found");
    }

    @Test
    public void testCheckKeyValidRetry() {
        assertThat(dagman.checkKey(Dagman.RETRY_KEY, "3"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidCategory() {
        assertThat(dagman.checkKey(Dagman.CATEGORY_KEY, "mycat"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidMaxIdle() {
        assertThat(dagman.checkKey(Dagman.MAXIDLE_KEY, "5"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidPriority() {
        assertThat(dagman.checkKey(Dagman.PRIORITY_KEY, "10"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyNullKeyThrowsNPE() {
        // checkKey sets res=MALFORMED_KEY but then calls key.charAt(0) which NPEs for null key
        assertThrows(NullPointerException.class, () -> dagman.checkKey(null, "value"));
    }

    @Test
    public void testCheckKeyShortKeyResultsInNotPermitted() {
        // "R" length < 2 sets MALFORMED_KEY, but switch still executes on charAt(0)='R'.
        // "R" != "RETRY" so switch sets NOT_PERMITTED_KEY.
        assertThat(dagman.checkKey("R", "3"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNullValueShortKeyResultsInNotPermitted() {
        // key="R" length < 2 sets MALFORMED_KEY, switch executes: "R" != "RETRY" → NOT_PERMITTED
        assertThat(dagman.checkKey("R", null), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedUnknown() {
        assertThat(dagman.checkKey("UNKNOWN_KEY", "value"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyValidPostScript() {
        assertThat(
                dagman.checkKey(Dagman.POST_SCRIPT_KEY, "/bin/pegasus-exitcode"),
                is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidPreScript() {
        assertThat(
                dagman.checkKey(Dagman.PRE_SCRIPT_KEY, "/bin/prescript.sh"),
                is(Namespace.VALID_KEY));
    }

    @Test
    public void testToStringContainsVarsKey() {
        // toCondor / toString should always add VARS key for retry
        String result = dagman.toCondor();
        assertThat(result, containsString("VARS"));
        assertThat(result, containsString("DAGNodeRetry"));
    }

    @Test
    public void testToCondorWithRetryKey() {
        dagman.construct(Dagman.RETRY_KEY, "3");
        String result = dagman.toCondor();
        assertThat(result, containsString("RETRY"));
        assertThat(result, containsString("test_job"));
        assertThat(result, containsString("3"));
    }

    @Test
    public void testClonePreservesValues() {
        dagman.construct(Dagman.RETRY_KEY, "5");
        Dagman clone = (Dagman) dagman.clone();
        assertThat((String) clone.get(Dagman.RETRY_KEY), is("5"));
    }

    @Test
    public void testCloneIsIndependent() {
        dagman.construct(Dagman.RETRY_KEY, "3");
        Dagman clone = (Dagman) dagman.clone();
        clone.construct(Dagman.PRIORITY_KEY, "10");
        assertFalse(
                dagman.containsKey(Dagman.PRIORITY_KEY),
                "Clone modification should not affect original");
    }

    @Test
    public void testMergeOverridesExistingKey() {
        dagman.construct(Dagman.RETRY_KEY, "2");
        Dagman other = new Dagman();
        other.construct(Dagman.RETRY_KEY, "5");
        dagman.merge(other);
        assertThat((String) dagman.get(Dagman.RETRY_KEY), is("5"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> dagman.merge(new ENV()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testCategoryRelatedKeyReturnsTrueForCategoryKey() {
        assertTrue(Dagman.categoryRelatedKey("SOME.CATEGORY"));
    }

    @Test
    public void testCategoryRelatedKeyReturnsFalseForPostScriptArguments() {
        assertFalse(Dagman.categoryRelatedKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY));
    }

    @Test
    public void testCategoryRelatedKeyReturnsFalseForPlainKey() {
        assertFalse(Dagman.categoryRelatedKey(Dagman.RETRY_KEY));
    }

    @Test
    public void testGetPOSTScriptPathReturnsNullWhenNotSet() {
        assertNull(dagman.getPOSTScriptPath("exitcode"));
    }
}
