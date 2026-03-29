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
package edu.isi.pegasus.aws.batch.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import joptsimple.OptionSet;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusAWSBatchTest {

    // A fresh instance is required per test because parseCommandLineOptions registers
    // options on the shared OptionParser; re-registering on the same instance throws.
    private PegasusAWSBatch client;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        client = new PegasusAWSBatch();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassExists() {
        assertNotNull(PegasusAWSBatch.class);
    }

    @Test
    public void testConstructorCreatesInstance() {
        assertNotNull(client);
    }

    @Test
    public void testParseCommandLineOptionsWithAllValueOptions() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {
                            "--prefix", "my-prefix",
                            "--account", "123456789",
                            "--region", "us-east-1",
                            "--compute-environment", "arn:aws:batch::111:compute-environment/my-ce",
                            "--job-definition", "arn:aws:batch::111:job-definition/my-def:1",
                            "--job-queue", "arn:aws:batch::111:job-queue/my-queue",
                            "--s3", "my-s3-bucket",
                            "--files", "file1.txt,file2.txt",
                            "--merge-logs", "merge-prefix"
                        });
        assertThat(options.valueOf("prefix"), is("my-prefix"));
        assertThat(options.valueOf("account"), is("123456789"));
        assertThat(options.valueOf("region"), is("us-east-1"));
        assertThat(
                options.valueOf("compute-environment"),
                is("arn:aws:batch::111:compute-environment/my-ce"));
        assertThat(
                options.valueOf("job-definition"),
                is("arn:aws:batch::111:job-definition/my-def:1"));
        assertThat(options.valueOf("job-queue"), is("arn:aws:batch::111:job-queue/my-queue"));
        assertThat(options.valueOf("s3"), is("my-s3-bucket"));
        assertThat(options.valueOf("files"), is("file1.txt,file2.txt"));
        assertThat(options.valueOf("merge-logs"), is("merge-prefix"));
    }

    @Test
    public void testParseCommandLineOptionsShortForms() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {
                            "-p", "my-prefix",
                            "-a", "111222333",
                            "-r", "us-west-2",
                            "-ce", "my-ce",
                            "-j", "my-jd",
                            "-q", "my-jq",
                            "-s", "my-bucket",
                            "-f", "a.txt",
                            "-m", "out-prefix"
                        });
        assertThat(options.valueOf("prefix"), is("my-prefix"));
        assertThat(options.valueOf("account"), is("111222333"));
        assertThat(options.valueOf("region"), is("us-west-2"));
        assertThat(options.valueOf("compute-environment"), is("my-ce"));
        assertThat(options.valueOf("job-definition"), is("my-jd"));
        assertThat(options.valueOf("job-queue"), is("my-jq"));
        assertThat(options.valueOf("s3"), is("my-bucket"));
        assertThat(options.valueOf("files"), is("a.txt"));
        assertThat(options.valueOf("merge-logs"), is("out-prefix"));
    }

    @Test
    public void testParseCommandLineOptionsFlagOptions() throws IOException {
        OptionSet createOptions =
                client.parseCommandLineOptions(new String[] {"--prefix", "p", "--create"});
        assertTrue(createOptions.has("create"));
        assertTrue(createOptions.has("c"));
        assertFalse(createOptions.has("delete"));

        // delete flag requires a fresh instance — re-registering options on the same parser throws
        PegasusAWSBatch deleteClient = new PegasusAWSBatch();
        OptionSet deleteOptions =
                deleteClient.parseCommandLineOptions(new String[] {"--prefix", "p", "--delete"});
        assertTrue(deleteOptions.has("delete"));
        assertTrue(deleteOptions.has("d"));
        assertFalse(deleteOptions.has("create"));
    }

    @Test
    public void testParseCommandLineOptionsLogFile() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {"--prefix", "p", "--log-file", "/tmp/batch.log"});
        assertTrue(options.has("log-file"));
        assertThat(options.valueOf("log-file"), is("/tmp/batch.log"));
    }

    @Test
    public void testParseCommandLineOptionsLogLevel() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {"--prefix", "p", "--log-level", "DEBUG"});
        assertTrue(options.has("log-level"));
        assertThat(options.valueOf("log-level"), is(Level.DEBUG));
    }

    @Test
    public void testParseCommandLineOptionsLogLevelCaseInsensitive() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {"--prefix", "p", "--log-level", "trace"});
        assertThat(options.valueOf("log-level"), is(Level.TRACE));
    }

    @Test
    public void testParseCommandLineOptionsNonOptionArgumentsAsSubmitFiles() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {"--prefix", "p", "submit1.json", "submit2.json"});
        List<?> nonOptions = options.nonOptionArguments();
        assertThat(nonOptions, hasSize(2));
        assertThat(nonOptions, contains("submit1.json", "submit2.json"));
    }

    @Test
    public void testParseCommandLineOptionsNoSubmitFilesWhenOnlyFlags() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(new String[] {"--prefix", "p", "--create"});
        assertThat(options.nonOptionArguments(), is(empty()));
    }

    @Test
    public void testParseCommandLineOptionsHelpFlag() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(new String[] {"--prefix", "p", "--help"});
        assertTrue(options.has("help"));
        assertTrue(options.has("h"));
    }

    @Test
    public void testParseCommandLineOptionsConfOption() throws IOException {
        OptionSet options =
                client.parseCommandLineOptions(
                        new String[] {
                            "--prefix", "p", "--conf", "/etc/pegasus/pegasus.properties"
                        });
        assertTrue(options.has("conf"));
        assertThat(options.valueOf("conf"), is("/etc/pegasus/pegasus.properties"));
    }

    @Test
    public void testParseCommandLineOptionsFlagsAbsentByDefault() throws IOException {
        OptionSet options = client.parseCommandLineOptions(new String[] {"--prefix", "p"});
        assertFalse(options.has("create"));
        assertFalse(options.has("delete"));
        assertFalse(options.has("help"));
        assertFalse(options.has("log-file"));
        assertFalse(options.has("log-level"));
    }
}
