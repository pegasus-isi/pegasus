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
public class GlobusTest {

    private Globus globus;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        globus = new Globus();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertTrue(globus.isEmpty(), "Newly constructed Globus namespace should be empty");
    }

    @Test
    public void testNamespaceName() {
        assertThat(globus.namespaceName(), is(Globus.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresLowerCaseKey() {
        globus.construct("COUNT", "4");
        assertTrue(globus.containsKey("count"), "Keys should be stored in lower case");
        assertFalse(globus.containsKey("COUNT"), "Upper case key should not be found");
    }

    @Test
    public void testCheckKeyValidCount() {
        assertThat(globus.checkKey(Globus.COUNT_KEY, "4"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidQueue() {
        assertThat(globus.checkKey(Globus.QUEUE_KEY, "default"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidMaxWalltime() {
        assertThat(globus.checkKey(Globus.MAX_WALLTIME_KEY, "3600"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidHostCount() {
        assertThat(globus.checkKey(Globus.HOST_COUNT_KEY, "2"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidProject() {
        assertThat(globus.checkKey(Globus.PROJECT_KEY, "myproject"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedArguments() {
        assertThat(globus.checkKey("arguments", "arg1"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedStdin() {
        assertThat(globus.checkKey("stdin", "/dev/null"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyMalformedNullKey() {
        assertThat(globus.checkKey(null, "value"), is(Namespace.MALFORMED_KEY));
    }

    @Test
    public void testCheckKeyEmptyValue() {
        assertThat(globus.checkKey(Globus.COUNT_KEY, ""), is(Namespace.EMPTY_KEY));
    }

    @Test
    public void testCheckKeyUnknownKey() {
        assertThat(globus.checkKey("unknownkey", "value"), is(Namespace.UNKNOWN_KEY));
    }

    @Test
    public void testToCondorContainsKeyValue() {
        globus.construct(Globus.COUNT_KEY, "4");
        String result = globus.toCondor();
        assertThat(result, containsString("(count=4)"));
    }

    @Test
    public void testToCondorFormatIsRSL() {
        globus.construct(Globus.QUEUE_KEY, "normal");
        String result = globus.toCondor();
        assertThat(result, containsString("(queue=normal)"));
    }

    @Test
    public void testToStringDelegatesToToCondor() {
        globus.construct(Globus.QUEUE_KEY, "normal");
        assertThat(globus.toString(), is(globus.toCondor()));
    }

    @Test
    public void testClonePreservesValues() {
        globus.construct(Globus.COUNT_KEY, "8");
        globus.construct(Globus.QUEUE_KEY, "normal");
        Globus clone = (Globus) globus.clone();
        assertThat((String) clone.get(Globus.COUNT_KEY), is("8"));
        assertThat((String) clone.get(Globus.QUEUE_KEY), is("normal"));
    }

    @Test
    public void testCloneIsIndependent() {
        globus.construct(Globus.COUNT_KEY, "4");
        Globus clone = (Globus) globus.clone();
        clone.construct(Globus.QUEUE_KEY, "high");
        assertFalse(
                globus.containsKey(Globus.QUEUE_KEY),
                "Clone modification should not affect original");
    }

    @Test
    public void testMergeUsesMaxAggregatorForMaxMemory() {
        globus.construct(Globus.MAX_MEMORY_KEY, "1024");
        Globus other = new Globus();
        other.construct(Globus.MAX_MEMORY_KEY, "2048");
        globus.merge(other);
        // MAX aggregator should keep the larger value
        assertThat((String) globus.get(Globus.MAX_MEMORY_KEY), is("2048"));
    }

    @Test
    public void testMergeUsesSumAggregatorForMaxWalltime() {
        globus.construct(Globus.MAX_WALLTIME_KEY, "100");
        Globus other = new Globus();
        other.construct(Globus.MAX_WALLTIME_KEY, "200");
        globus.merge(other);
        // Sum aggregator should add the values
        assertThat((String) globus.get(Globus.MAX_WALLTIME_KEY), is("300"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> globus.merge(new ENV()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testRslToPegasusProfilesMapIsPopulated() {
        assertFalse(Globus.rslToPegasusProfiles().isEmpty());
        assertTrue(Globus.rslToPegasusProfiles().containsKey(Globus.MAX_MEMORY_KEY));
    }
}
