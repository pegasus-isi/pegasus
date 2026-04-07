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
package edu.isi.pegasus.planner.code.gridstart;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for the Integrity class. */
public class IntegrityTest {

    @Test
    public void testIntegrityCheckToolBasename() {
        assertThat(Integrity.PEGASUS_INTEGRITY_CHECK_TOOL_BASENAME, is("pegasus-integrity"));
    }

    @Test
    public void testStdinFileDescriptor() {
        assertThat(Integrity.STDIN_FILE_DESCRIPTOR, is("stdin"));
    }

    @Test
    public void testIntegrityCanBeInstantiated() {
        Integrity integrity = new Integrity();
        assertThat(integrity, notNullValue());
    }

    @Test
    public void testIntegrityCheckToolBasenameNotNull() {
        assertThat(Integrity.PEGASUS_INTEGRITY_CHECK_TOOL_BASENAME, notNullValue());
    }

    @Test
    public void testAddIntegrityCheckInvocationIncludesEligibleFilesOnly() {
        Integrity integrity = new Integrity();

        PegasusFile raw = new PegasusFile("raw.dat");
        raw.setRawInput(true);
        raw.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "abc123");

        PegasusFile executable = new PegasusFile("tool.sh");
        executable.setType(PegasusFile.EXECUTABLE_FILE);

        PegasusFile checkpoint = new PegasusFile("checkpoint.dat");
        checkpoint.setType(PegasusFile.CHECKPOINT_FILE);
        checkpoint.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "skip");

        PegasusFile disabled = new PegasusFile("disabled.dat");
        disabled.setForIntegrityChecking(false);

        StringBuilder sb = new StringBuilder();
        String result =
                integrity.addIntegrityCheckInvocation(
                        sb, Arrays.asList(raw, executable, checkpoint, disabled));

        assertThat(result, is("raw.dat;;;tool.sh"));
        assertThat(sb.toString(), is("pegasus-integrity --print-timings --verify=stdin"));
    }

    @Test
    public void testAddIntegrityCheckInvocationReturnsEmptyWhenNoFilesNeedChecks() {
        Integrity integrity = new Integrity();

        PegasusFile rawWithoutChecksum = new PegasusFile("raw.dat");
        rawWithoutChecksum.setRawInput(true);

        PegasusFile other = new PegasusFile("other.dat");
        other.setType(PegasusFile.OTHER_FILE);

        StringBuilder sb = new StringBuilder();
        String result =
                integrity.addIntegrityCheckInvocation(sb, Arrays.asList(rawWithoutChecksum, other));

        assertThat(result, is(""));
        assertThat(sb.toString(), is(""));
    }

    @Test
    public void testGenerateChecksumMetadataFileReturnsNullWhenNoMetadataExists() {
        Integrity integrity = new Integrity();
        PegasusFile file = new PegasusFile("f1");

        assertThat(
                integrity.generateChecksumMetadataFile(
                        "build/integrity-no-metadata.json", Collections.singletonList(file)),
                nullValue());
    }

    @Test
    public void testGenerateChecksumMetadataFileWritesExpectedJson() throws Exception {
        Integrity integrity = new Integrity();

        PegasusFile data = new PegasusFile("input.dat");
        data.addMetadata(Metadata.CHECKSUM_TYPE_KEY, "sha256");
        data.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "abc123");

        PegasusFile executable = new PegasusFile("tool.sh");
        executable.setType(PegasusFile.EXECUTABLE_FILE);
        executable.addMetadata("custom", "value");

        Path tempDir = Files.createTempDirectory("integrity-test");
        Path metadataFilePath = tempDir.resolve("inputs.meta");

        java.io.File metadataFile =
                integrity.generateChecksumMetadataFile(
                        metadataFilePath.toString(), Arrays.asList(data, executable));

        assertThat(metadataFile, notNullValue());
        assertThat(metadataFile.getAbsolutePath(), is(metadataFilePath.toFile().getAbsolutePath()));

        String json = Files.readString(metadataFilePath);
        assertThat(json, containsString("\"_id\": \"input.dat\""));
        assertThat(json, containsString("\"_type\": \"data\""));
        assertThat(json, containsString("\"checksum.type\":\"sha256\""));
        assertThat(json, containsString("\"checksum.value\":\"abc123\""));
        assertThat(json, containsString("\"_id\": \"tool.sh\""));
        assertThat(json, containsString("\"_type\": \"executable\""));
        assertThat(json, containsString("\"custom\":\"value\""));
    }
}
