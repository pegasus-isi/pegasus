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
package edu.isi.pegasus.planner.code.generator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PlannerMetrics;
import edu.isi.pegasus.planner.namespace.ENV;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Metrics code generator class. */
public class MetricsTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    @Test
    public void testMetricsFileSuffixConstant() {
        assertThat(Metrics.METRICS_FILE_SUFFIX, is(".metrics"));
    }

    @Test
    public void testMetricsServerDefaultURL() {
        assertThat(Metrics.METRICS_SERVER_DEFAULT_URL, startsWith("http"));
    }

    @Test
    public void testCollectMetricsEnvVariableConstant() {
        assertThat(Metrics.COLLECT_METRICS_ENV_VARIABLE, is("PEGASUS_METRICS"));
    }

    @Test
    public void testPrimaryMetricsServerURLEnvVariable() {
        assertThat(Metrics.PRIMARY_METRICS_SERVER_URL_ENV_VARIABLE, is("PEGASUS_METRICS_SERVER"));
    }

    @Test
    public void testSecondaryMetricsServerURLEnvVariable() {
        assertThat(
                Metrics.SECONDARY_METRICS_SERVER_URL_ENV_VARIABLE,
                is("PEGASUS_USER_METRICS_SERVER"));
    }

    @Test
    public void testDagmanMetricsEnvVariable() {
        assertThat(Metrics.DAGMAN_METRICS_ENV_VARIABLE, is("PEGASUS_METRICS"));
    }

    @Test
    public void testMetricsSendTimeoutIsPositive() {
        assertThat(Metrics.METRICS_SEND_TIMEOUT > 0, is(true));
    }

    @Test
    public void testConstructorInitializesDefaultState() throws Exception {
        Metrics metrics = new Metrics();

        assertThat(
                (Boolean) ReflectionTestUtils.getField(metrics, "mSendMetricsToServer"), is(true));
        assertThat(
                ((List<?>) ReflectionTestUtils.getField(metrics, "mMetricsServers")).isEmpty(),
                is(true));
    }

    @Test
    public void testInitializeUsesDefaultMetricsServerAndLogger() throws Exception {
        Metrics metrics = new Metrics();

        metrics.initialize(null);

        assertThat(
                ReflectionTestUtils.getField(metrics, "mSendMetricsToServer"),
                is(Metrics.ENABLE_METRICS_REPORTING()));
        assertThat(ReflectionTestUtils.getField(metrics, "mLogger"), notNullValue());
        assertThat(
                ((List<?>) ReflectionTestUtils.getField(metrics, "mMetricsServers")).size(), is(1));
        assertThat(
                ((List<?>) ReflectionTestUtils.getField(metrics, "mMetricsServers")).get(0),
                is(Metrics.METRICS_SERVER_DEFAULT_URL));
    }

    @Test
    public void testGetDAGManMetricsEnvReturnsEmptyWhenMetricsDisabled() throws Exception {
        Metrics metrics = new Metrics();
        ReflectionTestUtils.setField(metrics, "mSendMetricsToServer", false);

        ENV env = metrics.getDAGManMetricsEnv();

        assertThat(env.get(Metrics.DAGMAN_METRICS_ENV_VARIABLE), nullValue());
        assertThat(env.toCondor(), nullValue());
    }

    @Test
    public void testGetDAGManMetricsEnvSetsEnableFlagWhenMetricsEnabled() throws Exception {
        Metrics metrics = new Metrics();
        ReflectionTestUtils.setField(metrics, "mSendMetricsToServer", true);
        ReflectionTestUtils.setField(metrics, "mLogger", new NoOpLogManager());

        ENV env = metrics.getDAGManMetricsEnv();

        assertThat(env.get(Metrics.DAGMAN_METRICS_ENV_VARIABLE), is("true"));
    }

    @Test
    public void testLogMetricsWritesMetricsFileWhenServerSendingDisabled() throws Exception {
        Metrics generator = new Metrics();
        ReflectionTestUtils.setField(generator, "mSendMetricsToServer", false);
        ReflectionTestUtils.setField(generator, "mLogger", new NoOpLogManager());
        PlannerMetrics metrics = new PlannerMetrics();
        File file = Files.createTempFile("planner-metrics", ".json").toFile();
        metrics.setMetricsFileLocationInSubmitDirectory(file);

        generator.logMetrics(metrics);

        String contents = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        assertThat(file.exists(), is(true));
        assertThat(contents, containsString("\"type\": \"metrics\""));
        assertThat(contents.endsWith("\n\n"), is(true));
    }

    @Test
    public void testWriteOutMetricsFileRejectsNullMetrics() throws Exception {
        Metrics metrics = new Metrics();
        Method method =
                Metrics.class.getDeclaredMethod("writeOutMetricsFile", PlannerMetrics.class);
        method.setAccessible(true);

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> method.invoke(metrics, new Object[] {null}));

        assertThat(exception.getCause(), instanceOf(java.io.IOException.class));
        assertThat(exception.getCause().getMessage(), is("NULL Metrics passed"));
    }

    @Test
    public void testWriteOutMetricsFileRejectsMissingMetricsFileLocation() throws Exception {
        Metrics metrics = new Metrics();
        Method method =
                Metrics.class.getDeclaredMethod("writeOutMetricsFile", PlannerMetrics.class);
        method.setAccessible(true);
        PlannerMetrics plannerMetrics = new PlannerMetrics();

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> method.invoke(metrics, plannerMetrics));

        assertThat(exception.getCause(), instanceOf(java.io.IOException.class));
        assertThat(
                exception.getCause().getMessage(),
                is("The metrics file location is not yet initialized"));
    }
}
