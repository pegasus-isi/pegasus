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
public class CondorTest {

    private Condor condor;

    @BeforeEach
    public void setUp() {
        condor = new Condor();
    }

    @Test
    public void testDefaultConstructor() {
        org.hamcrest.MatcherAssert.assertThat(
                "Newly constructed Condor namespace should be empty",
                condor.isEmpty(),
                org.hamcrest.Matchers.is(true));
        assertThat(condor.namespaceName(), is(Condor.NAMESPACE_NAME));
    }

    @Test
    public void testConstructStoresLowerCaseKey() {
        condor.construct("Universe", "vanilla");
        org.hamcrest.MatcherAssert.assertThat(
                "Keys should be stored in lower case",
                condor.containsKey("universe"),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                "Upper case key should not be found",
                condor.containsKey("Universe"),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testConstructPreservesClassAdKeyCase() {
        condor.construct("+ProjectName", "MyProject");
        org.hamcrest.MatcherAssert.assertThat(
                "ClassAd keys starting with + should preserve case",
                condor.containsKey("+ProjectName"),
                org.hamcrest.Matchers.is(true));
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
        org.hamcrest.MatcherAssert.assertThat(
                "Clone modification should not affect original",
                condor.containsKey("notification"),
                org.hamcrest.Matchers.is(false));
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
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.TRANSFER_IP_FILES_KEY), org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey("should_transfer_files"), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testGetIPFilesForTransferReturnsNullWhenNotSet() {
        org.hamcrest.MatcherAssert.assertThat(
                condor.getIPFilesForTransfer(), org.hamcrest.Matchers.nullValue());
    }

    @Test
    public void testGetIPFilesForTransferReturnsValue() {
        condor.construct(Condor.TRANSFER_IP_FILES_KEY, "/path/file.txt");
        assertThat(condor.getIPFilesForTransfer(), is("/path/file.txt"));
    }

    @Test
    public void testClassAdKeysToPegasusProfilesContainsExpectedMappings() {
        assertThat(
                Condor.classAdKeysToPegasusProfiles().get(Condor.REQUEST_MEMORY_KEY),
                is(Pegasus.MEMORY_KEY));
        assertThat(
                Condor.classAdKeysToPegasusProfiles().get(Condor.REQUEST_CPUS_KEY),
                is(Pegasus.CORES_KEY));
        assertThat(
                Condor.classAdKeysToPegasusProfiles().get(Condor.REQUEST_GPUS_KEY),
                is(Pegasus.GPUS_KEY));
        assertThat(
                Condor.classAdKeysToPegasusProfiles().get(Condor.REQUEST_DISK_KEY),
                is(Pegasus.DISKSPACE_KEY));
    }

    @Test
    public void testExpandFilePathReplacesClassAdReference() {
        assertThat(
                Condor.expandFilePath(
                        "$(wf_submit_dir)/00/01/file.txt", Condor.WF_SUBMIT_DIR_KEY, "/submit"),
                is("/submit/00/01/file.txt"));
    }

    @Test
    public void testExpandFilePathReturnsOriginalWhenNoReferencePresent() {
        assertThat(
                Condor.expandFilePath("/plain/path/file.txt", Condor.WF_SUBMIT_DIR_KEY, "/submit"),
                is("/plain/path/file.txt"));
    }

    @Test
    public void testRemoveIPFilesForTransferRemovesAssociatedCondorKeysWhenUnused() {
        condor.addIPFileForTransfer("/data/input.txt");

        condor.removeIPFilesForTransfer();

        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.TRANSFER_IP_FILES_KEY), org.hamcrest.Matchers.is(false));
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.SHOULD_TRANSFER_FILES_KEY),
                org.hamcrest.Matchers.is(false));
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testRemoveIPFilesForTransferPreservesAssociatedKeysWhenOutputFilesRemain() {
        condor.addIPFileForTransfer("/data/input.txt");
        condor.addOPFileForTransfer("output.txt");

        condor.removeIPFilesForTransfer();

        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.TRANSFER_IP_FILES_KEY), org.hamcrest.Matchers.is(false));
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.SHOULD_TRANSFER_FILES_KEY),
                org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                condor.containsKey(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testAddOPFileForTransferRemapAppendsWithSemicolonSeparator() {
        condor.addOPFileForTransferRemap("a.txt", "b.txt");
        condor.addOPFileForTransferRemap("c.txt", "d.txt");

        assertThat(condor.getOutputRemapsForTransfer(), is("a.txt=b.txt;c.txt=d.txt"));
    }

    @Test
    public void testCheckKeyInNSForFilesystemDomainConstructsRequirementsExpression() {
        condor.checkKeyInNS(Condor.FILE_SYSTEM_DOMAIN_KEY, "example.org");

        assertThat(
                (String) condor.get(Condor.REQUIREMENTS_KEY),
                is("filesystemdomain == \"example.org\""));
    }

    @Test
    public void testCheckKeyInNSConvertsRequestMemoryUnitsToMegabytes() {
        condor.checkKeyInNS(Condor.REQUEST_MEMORY_KEY, "1 GB");

        assertThat((String) condor.get(Condor.REQUEST_MEMORY_KEY), is("1024"));
    }

    @Test
    public void testCheckKeyInNSConvertsRequestDiskUnitsToKilobytes() {
        condor.checkKeyInNS(Condor.REQUEST_DISK_KEY, "2 MB");

        assertThat((String) condor.get(Condor.REQUEST_DISK_KEY), is("2048"));
    }
}
