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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link YAMLDAX2Metadata}. */
public class YAMLDAX2MetadataTest {

    @TempDir Path tempDir;

    @Test
    public void getMetadataReturnsDefaultsAndTopLevelFields() throws IOException {
        Path dax =
                Files.write(
                        tempDir.resolve("workflow.yml"),
                        ("pegasus: '5.0.4'\n"
                                        + "name: local-hierarchy\n"
                                        + "jobs:\n"
                                        + "  - type: pegasusWorkflow\n")
                                .getBytes(StandardCharsets.UTF_8));

        Map<String, String> metadata = YAMLDAX2Metadata.getMetadata(null, dax.toString());

        assertThat(metadata.get("index"), is("0"));
        assertThat(metadata.get("count"), is("1"));
        assertThat(metadata.get("version"), is("5.0.4"));
        assertThat(metadata.get("name"), is("local-hierarchy"));
        assertThat(metadata.size(), is(4));
    }

    @Test
    public void getMetadataStopsOnceJobsSectionIsReached() throws IOException {
        Path dax =
                Files.write(
                        tempDir.resolve("short-circuit.yml"),
                        ("pegasus: '5.0.4'\n"
                                        + "jobs:\n"
                                        + "  - type: pegasusWorkflow\n"
                                        + "name: ignored-after-jobs\n")
                                .getBytes(StandardCharsets.UTF_8));

        Map<String, String> metadata = YAMLDAX2Metadata.getMetadata(null, dax.toString());

        assertThat(metadata.get("version"), is("5.0.4"));
        assertThat(metadata.get("index"), is("0"));
        assertThat(metadata.get("count"), is("1"));
        assertThat(metadata.containsKey("name"), is(false));
    }

    @Test
    public void getMetadataWrapsIoErrorsInRuntimeException() {
        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                YAMLDAX2Metadata.getMetadata(
                                        null, tempDir.resolve("missing.yml").toString()));

        assertThat(exception.getMessage().contains("Exception while reading file"), is(true));
        assertThat(exception.getCause(), instanceOf(IOException.class));
    }
}
