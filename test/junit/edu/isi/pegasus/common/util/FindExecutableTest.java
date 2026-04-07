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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RestoreSystemProperties;

/** @author Rajiv Mayani */
public class FindExecutableTest {

    @Test
    public void testFindExecReturnsNullForNullName() {
        assertThat(FindExecutable.findExec((String) null), is(nullValue()));
        assertThat(FindExecutable.findExec("/tmp", null), is(nullValue()));
    }

    @Test
    public void testFindExecFindsExecutableInPreferredDirectory() throws Exception {
        Path directory = Files.createTempDirectory("findexec");
        Path executable = directory.resolve("tool.sh");
        Files.write(executable, "#!/bin/sh\nexit 0\n".getBytes());
        executable.toFile().setExecutable(true);

        File found = FindExecutable.findExec(directory.toString(), "tool.sh");

        assertThat(found, is(notNullValue()));
        assertThat(found.getAbsolutePath(), is(executable.toFile().getAbsolutePath()));
    }

    @Test
    public void testFindExecSkipsNonExecutableFiles() throws Exception {
        Path directory = Files.createTempDirectory("findexec");
        Path file = directory.resolve("not-executable.sh");
        Files.write(file, "echo hi\n".getBytes());
        file.toFile().setExecutable(false);

        assertThat(
                FindExecutable.findExec(directory.toString(), "not-executable.sh"),
                is(nullValue()));
    }

    @Test
    @RestoreSystemProperties
    public void testFindExecUsesPegasusHomeBindirProperty() throws Exception {
        Path directory = Files.createTempDirectory("findexec");
        Path executable = directory.resolve("pegasus-tool");
        Files.write(executable, "#!/bin/sh\nexit 0\n".getBytes());
        executable.toFile().setExecutable(true);

        System.setProperty("pegasus.home.bindir", directory.toString());
        File found = FindExecutable.findExec("pegasus-tool");

        assertThat(found, is(notNullValue()));
        assertThat(found.getAbsolutePath(), is(executable.toFile().getAbsolutePath()));
    }

    @Test
    @RestoreSystemProperties
    public void testMainPrintsFoundAndNotFoundResults() throws Exception {
        Path directory = Files.createTempDirectory("findexec");
        Path executable = directory.resolve("main-tool");
        Files.write(executable, "#!/bin/sh\nexit 0\n".getBytes());
        executable.toFile().setExecutable(true);
        String missing = "missing-tool-" + System.nanoTime();

        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setProperty("pegasus.home.bindir", directory.toString());
            System.setOut(new PrintStream(output));

            FindExecutable.main(new String[] {"main-tool", missing});
        } finally {
            System.setOut(originalOut);
        }

        String text = output.toString();
        assertThat(text.contains("main-tool -> "), is(true));
        assertThat(text.contains(missing + " not found"), is(true));
    }
}
