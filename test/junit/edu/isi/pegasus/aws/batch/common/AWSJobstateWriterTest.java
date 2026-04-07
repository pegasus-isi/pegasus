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
package edu.isi.pegasus.aws.batch.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.aws.batch.classes.AWSJob;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class AWSJobstateWriterTest {

    @TempDir File tempDir;

    @Test
    public void testClassCanBeInstantiated() {
        AWSJobstateWriter writer = new AWSJobstateWriter();
        assertThat(writer, is(notNullValue()));
    }

    @Test
    public void testJobstateLogFilenameConstant() {
        assertThat(AWSJobstateWriter.JOBSTATE_LOG_FILENAME, is(".jobstate.log"));
    }

    @Test
    public void testInitializeCreatesLogFile() {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        writer.initialze(tempDir, "test-", logger);
        File logFile = new File(tempDir, "test-" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        assertThat(logFile.exists(), is(true));
    }

    @Test
    public void testLogWritesEntry() throws IOException {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        writer.initialze(tempDir, "run-", logger);
        writer.log("myjob", "aws-id-123", AWSJob.JOBSTATE.running);
        File logFile = new File(tempDir, "run-" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        String content = new String(Files.readAllBytes(logFile.toPath()));
        assertThat(content, containsString("myjob"));
        assertThat(content, containsString("aws-id-123"));
        assertThat(content, containsString("RUNNING"));
    }

    @Test
    public void testLogStateIsUppercase() throws IOException {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        writer.initialze(tempDir, "state-", logger);
        writer.log("job1", "aws-1", AWSJob.JOBSTATE.failed);
        File logFile = new File(tempDir, "state-" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        String content = new String(Files.readAllBytes(logFile.toPath()));
        assertThat(content, containsString("FAILED"));
        assertThat(content, not(containsString("failed")));
    }

    @Test
    public void testLogWritesMultipleEntries() throws IOException {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        writer.initialze(tempDir, "multi-", logger);
        writer.log("job-a", "aws-aaa", AWSJob.JOBSTATE.submitted);
        writer.log("job-b", "aws-bbb", AWSJob.JOBSTATE.running);
        writer.log("job-c", "aws-ccc", AWSJob.JOBSTATE.succeeded);
        File logFile = new File(tempDir, "multi-" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        String content = new String(Files.readAllBytes(logFile.toPath()));
        assertThat(content, containsString("job-a"));
        assertThat(content, containsString("aws-aaa"));
        assertThat(content, containsString("job-b"));
        assertThat(content, containsString("aws-bbb"));
        assertThat(content, containsString("job-c"));
        assertThat(content, containsString("aws-ccc"));
        assertThat(content, containsString("SUCCEEDED"));
    }

    @Test
    public void testLogWithoutInitializeThrows() {
        AWSJobstateWriter writer = new AWSJobstateWriter();
        assertThrows(Exception.class, () -> writer.log("job", "aws-id", AWSJob.JOBSTATE.running));
    }

    @Test
    public void testInitializeThrowsForNonExistentDirectory() {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        File nonExistent = new File(tempDir, "no/such/dir");
        assertThrows(RuntimeException.class, () -> writer.initialze(nonExistent, "p-", logger));
    }

    @Test
    public void testInitializeWithNullPrefixUsesLiteralNullInFilename() {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();

        writer.initialze(tempDir, null, logger);

        File logFile = new File(tempDir, "null" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        assertThat(logFile.exists(), is(true));
    }

    @Test
    public void testInitializeAssignsClassLoggerIgnoringPassedLogger() throws Exception {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();

        writer.initialze(tempDir, "logger-", logger);

        Logger assigned = (Logger) ReflectionTestUtils.getField(writer, "mLogger");

        assertThat(assigned, is(notNullValue()));
        assertThat(assigned.getName(), is(AWSJobstateWriter.class.getName()));
    }

    @Test
    public void testLogLineUsesFourWhitespaceSeparatedTokens() throws IOException {
        Logger logger = LogManager.getLogger(AWSJobstateWriterTest.class);
        AWSJobstateWriter writer = new AWSJobstateWriter();
        writer.initialze(tempDir, "tokens-", logger);
        writer.log("job-x", "aws-xyz", AWSJob.JOBSTATE.submitted);

        File logFile = new File(tempDir, "tokens-" + AWSJobstateWriter.JOBSTATE_LOG_FILENAME);
        String line = Files.readAllLines(logFile.toPath()).get(0);
        String[] tokens = line.split(" ");

        assertThat(tokens.length, is(4));
        assertThat(tokens[1], is("job-x"));
        assertThat(tokens[2], is("SUBMITTED"));
        assertThat(tokens[3], is("aws-xyz"));
    }
}
