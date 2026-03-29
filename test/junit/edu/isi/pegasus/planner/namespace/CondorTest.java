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
public class CondorTest {

    private Condor condor;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        condor = new Condor();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructor() {
        assertTrue(condor.isEmpty(), "Newly constructed Condor namespace should be empty");
        assertThat(condor.namespaceName(), is(Condor.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresLowerCaseKey() {
        condor.construct("Universe", "vanilla");
        assertTrue(condor.containsKey("universe"), "Keys should be stored in lower case");
        assertFalse(condor.containsKey("Universe"), "Upper case key should not be found");
    }

    @Test
    public void testConstructPreservesClassAdKeyCase() {
        condor.construct("+ProjectName", "MyProject");
        assertTrue(
                condor.containsKey("+ProjectName"),
                "ClassAd keys starting with + should preserve case");
    }

    @Test
    public void testCheckKeyValidUniverse() {
        assertThat(condor.checkKey(Condor.UNIVERSE_KEY, "vanilla"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidRequestCpus() {
        assertThat(condor.checkKey(Condor.REQUEST_CPUS_KEY, "4"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyValidRequestMemory() {
        assertThat(condor.checkKey(Condor.REQUEST_MEMORY_KEY, "1024"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedArguments() {
        assertThat(condor.checkKey("arguments", "arg1 arg2"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyNotPermittedQueue() {
        assertThat(condor.checkKey("queue", "1"), is(Namespace.NOT_PERMITTED_KEY));
    }

    @Test
    public void testCheckKeyMalformedNullKey() {
        assertThat(condor.checkKey(null, "value"), is(Namespace.MALFORMED_KEY));
    }

    @Test
    public void testCheckKeyEmptyValue() {
        assertThat(condor.checkKey(Condor.UNIVERSE_KEY, ""), is(Namespace.EMPTY_KEY));
    }

    @Test
    public void testCheckKeyValidClassAdKey() {
        assertThat(condor.checkKey("+ProjectName", "MyProject"), is(Namespace.VALID_KEY));
    }

    @Test
    public void testCheckKeyUnknown() {
        assertThat(condor.checkKey("some_unknown_key", "value"), is(Namespace.UNKNOWN_KEY));
    }

    @Test
    public void testToCondorContainsKeyValue() {
        condor.construct("universe", "vanilla");
        condor.construct("notification", "Never");
        String result = condor.toCondor();
        assertThat(result, containsString("universe = vanilla"));
        assertThat(result, containsString("notification = Never"));
    }

    @Test
    public void testToStringDelegatesToToCondor() {
        condor.construct("universe", "vanilla");
        assertThat(condor.toString(), is(condor.toCondor()));
    }

    @Test
    public void testCloneIsIndependent() {
        condor.construct("universe", "vanilla");
        Condor clone = (Condor) condor.clone();
        clone.construct("notification", "Never");
        assertFalse(
                condor.containsKey("notification"),
                "Clone modification should not affect original");
    }

    @Test
    public void testClonePreservesValues() {
        condor.construct("universe", "vanilla");
        condor.construct("priority", "10");
        Condor clone = (Condor) condor.clone();
        assertThat((String) clone.get("universe"), is("vanilla"));
        assertThat((String) clone.get("priority"), is("10"));
    }

    @Test
    public void testMergeOverridesExistingKey() {
        condor.construct("universe", "vanilla");
        Condor other = new Condor();
        other.construct("universe", "grid");
        condor.merge(other);
        assertThat((String) condor.get("universe"), is("grid"));
    }

    @Test
    public void testMergeThrowsOnWrongType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> condor.merge(new ENV()),
                "Merging incompatible namespace types should throw");
    }

    @Test
    public void testAddIPFileForTransfer() {
        condor.addIPFileForTransfer("/data/input.txt");
        assertTrue(condor.containsKey(Condor.TRANSFER_IP_FILES_KEY));
        assertTrue(condor.containsKey("should_transfer_files"));
    }

    @Test
    public void testGetIPFilesForTransferReturnsNullWhenNotSet() {
        assertNull(condor.getIPFilesForTransfer());
    }

    @Test
    public void testGetIPFilesForTransferReturnsValue() {
        condor.construct(Condor.TRANSFER_IP_FILES_KEY, "/path/file.txt");
        assertThat(condor.getIPFilesForTransfer(), is("/path/file.txt"));
    }
}
