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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DagmanTest {

    private Dagman dagman;

    @BeforeEach
    public void setUp() {
        dagman = new Dagman();
        dagman.setJobName("test_job");
    }

    @Test
    public void testDefaultConstructor() {
        Dagman d = new Dagman();
        assertThat(d.namespaceName(), is(Dagman.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresUpperCaseKey() {
        dagman.construct("retry", "3");
        org.hamcrest.MatcherAssert.assertThat(
                "Keys should be stored in UPPER CASE",
                dagman.containsKey("RETRY"),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                "Lower case key should not be found",
                dagman.containsKey("retry"),
                org.hamcrest.Matchers.is(false));
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
        org.hamcrest.MatcherAssert.assertThat(
                "Clone modification should not affect original",
                dagman.containsKey(Dagman.PRIORITY_KEY),
                org.hamcrest.Matchers.is(false));
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
        org.hamcrest.MatcherAssert.assertThat(
                Dagman.categoryRelatedKey("SOME.CATEGORY"), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testCategoryRelatedKeyReturnsFalseForPostScriptArguments() {
        org.hamcrest.MatcherAssert.assertThat(
                Dagman.categoryRelatedKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testCategoryRelatedKeyReturnsFalseForPlainKey() {
        org.hamcrest.MatcherAssert.assertThat(
                Dagman.categoryRelatedKey(Dagman.RETRY_KEY), org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testGetPOSTScriptPathReturnsNullWhenNotSet() {
        org.hamcrest.MatcherAssert.assertThat(
                dagman.getPOSTScriptPath("exitcode"), org.hamcrest.Matchers.nullValue());
    }

    @Test
    public void testGetPOSTScriptPathReturnsConfiguredTypeSpecificPath() {
        dagman.construct(Dagman.POST_SCRIPT_PATH_PREFIX + ".EXITCODE", "/bin/pegasus-exitcode");

        assertThat(dagman.getPOSTScriptPath("exitcode"), is("/bin/pegasus-exitcode"));
    }

    @Test
    public void testToStringReplacesScriptKeysAndAppendsArguments() {
        dagman.construct(Dagman.POST_SCRIPT_KEY, "/bin/post.sh");
        dagman.construct(Dagman.POST_SCRIPT_ARGUMENTS_KEY, "--post");
        dagman.construct(Dagman.OUTPUT_KEY, "post.out");
        dagman.construct(Dagman.PRE_SCRIPT_KEY, "/bin/pre.sh");
        dagman.construct(Dagman.PRE_SCRIPT_ARGUMENTS_KEY, "--pre");

        String result = dagman.toCondor();

        assertThat(result, containsString("SCRIPT POST test_job /bin/post.sh --post post.out"));
        assertThat(result, containsString("SCRIPT PRE test_job /bin/pre.sh --pre"));
    }

    @Test
    public void testToStringForJobKeyAppendsNoopWhenPresent() {
        dagman.construct(Dagman.JOB_KEY, "job.sub");
        dagman.construct(Dagman.NOOP_KEY, "true");

        String result = dagman.toCondor();

        assertThat(result, containsString("JOB test_job job.sub NOOP"));
    }

    @Test
    public void testToStringUsesProvidedClassAdPrefixForVars() {
        String result = dagman.toString("test_job", "+");

        assertThat(result, containsString("VARS test_job +DAGNodeRetry=\"$(RETRY)\""));
    }

    @Test
    public void testCheckKeyInNSIgnoresCategoryRelatedKey() {
        dagman.mLogger.logEventStart("test.namespace.Dagman", "set", "0");
        try {
            dagman.checkKeyInNS("SOME.CATEGORY", "ignored");
            org.hamcrest.MatcherAssert.assertThat(
                    dagman.containsKey("SOME.CATEGORY"), org.hamcrest.Matchers.is(false));
        } finally {
            dagman.mLogger.logEventCompletion();
        }
    }
}
