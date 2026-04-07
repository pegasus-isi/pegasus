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

import edu.isi.pegasus.planner.namespace.aggregator.MAX;
import edu.isi.pegasus.planner.namespace.aggregator.Sum;
import edu.isi.pegasus.planner.namespace.aggregator.Update;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class GlobusTest {

    private Globus globus;

    @BeforeEach
    public void setUp() {
        globus = new Globus();
    }

    @Test
    public void testDefaultConstructorIsEmpty() {
        org.hamcrest.MatcherAssert.assertThat(
                "Newly constructed Globus namespace should be empty",
                globus.isEmpty(),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testNamespaceName() {
        assertThat(globus.namespaceName(), is(Globus.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresLowerCaseKey() {
        globus.construct("COUNT", "4");
        org.hamcrest.MatcherAssert.assertThat(
                "Keys should be stored in lower case",
                globus.containsKey("count"),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                "Upper case key should not be found",
                globus.containsKey("COUNT"),
                org.hamcrest.Matchers.is(false));
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
        org.hamcrest.MatcherAssert.assertThat(
                "Clone modification should not affect original",
                globus.containsKey(Globus.QUEUE_KEY),
                org.hamcrest.Matchers.is(false));
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
        org.hamcrest.MatcherAssert.assertThat(
                Globus.rslToPegasusProfiles().isEmpty(), org.hamcrest.Matchers.is(false));
        org.hamcrest.MatcherAssert.assertThat(
                Globus.rslToPegasusProfiles().containsKey(Globus.MAX_MEMORY_KEY),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testPegasusProfilesToRSLKeyContainsExpectedMappings() {
        assertThat(
                Globus.pegasusProfilesToRSLKey().get(Pegasus.MEMORY_KEY),
                is(Globus.MAX_MEMORY_KEY));
        assertThat(
                Globus.pegasusProfilesToRSLKey().get(Pegasus.RUNTIME_KEY),
                is(Globus.MAX_WALLTIME_KEY));
        assertThat(Globus.pegasusProfilesToRSLKey().get(Pegasus.CORES_KEY), is(Globus.COUNT_KEY));
    }

    @Test
    public void testRslToEnvProfilesContainsExpectedMappings() {
        assertThat(Globus.rslToEnvProfiles().get(Globus.MAX_MEMORY_KEY), is("PEGASUS_MEMORY"));
        assertThat(Globus.rslToEnvProfiles().get(Globus.MAX_WALLTIME_KEY), is("PEGASUS_RUNTIME"));
        assertThat(Globus.rslToEnvProfiles().get(Globus.QUEUE_KEY), is("PEGASUS_QUEUE"));
    }

    @Test
    public void testRslKeysSubstitutedWithPegasusClassAdsContainsExpectedKeys() {
        org.hamcrest.MatcherAssert.assertThat(
                Globus.rslKeysSubstitutedWithPegasusClassAds().contains(Globus.COUNT_KEY),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                Globus.rslKeysSubstitutedWithPegasusClassAds().contains(Globus.MAX_MEMORY_KEY),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                Globus.rslKeysSubstitutedWithPegasusClassAds().contains(Globus.MAX_WALLTIME_KEY),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testMergeUsesDefaultUpdateAggregatorForQueue() {
        globus.construct(Globus.QUEUE_KEY, "normal");
        Globus other = new Globus();
        other.construct(Globus.QUEUE_KEY, "debug");

        globus.merge(other);

        assertThat((String) globus.get(Globus.QUEUE_KEY), is("debug"));
    }

    @Test
    public void testToCondorAppendsRawRslAndSkipsEmptyValues() {
        globus.construct("rsl", "(custom=true)");
        globus.construct(Globus.QUEUE_KEY, "");
        globus.construct(Globus.COUNT_KEY, "4");

        String result = globus.toCondor();

        assertThat(result, containsString("(custom=true)"));
        assertThat(result, containsString("(count=4)"));
        org.hamcrest.MatcherAssert.assertThat(
                result.contains("(queue=)"), org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testAggregatorSelectionUsesSpecificAndDefaultAggregators() {
        TestGlobus testGlobus = new TestGlobus();

        org.hamcrest.MatcherAssert.assertThat(
                testGlobus.aggregatorFor(Globus.MAX_MEMORY_KEY) instanceof MAX,
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                testGlobus.aggregatorFor(Globus.MAX_WALLTIME_KEY) instanceof Sum,
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                testGlobus.aggregatorFor(Globus.QUEUE_KEY) instanceof Update,
                org.hamcrest.Matchers.is(true));
    }

    private static final class TestGlobus extends Globus {
        Object aggregatorFor(String key) {
            return this.aggregator(key);
        }
    }
}
