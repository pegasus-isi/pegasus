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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.POSTScript;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusExitCodeTest {

    @Test
    public void testConstantsAndImplementsPOSTScript() {
        assertThat(
                PegasusExitCode.POSTSCRIPT_ARGUMENTS_FOR_PASSING_DAGMAN_JOB_EXITCODE,
                is("-r $RETURN"));
        assertThat(
                PegasusExitCode.POSTSCRIPT_ARGUMENTS_FOR_DISABLING_CHECKS_FOR_INVOCATIONS,
                is("--no-invocations"));
        assertThat(PegasusExitCode.SHORT_NAME, is("pegasus-exitcode"));
        assertThat(PegasusExitCode.ERR_SUCCESS_MSG_DELIMITER, notNullValue());
        assertThat(POSTScript.class.isAssignableFrom(PegasusExitCode.class), is(true));
    }

    @Test
    public void testInitializeSetsExpectedFieldsAndShortDescribe() throws Exception {
        PegasusExitCode exitCode = new PegasusExitCode();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        exitCode.initialize(props, "/custom/pegasus-exitcode", "/submit/dir", "workflow.log");

        assertThat(exitCode.shortDescribe(), is("pegasus-exitcode"));
        assertThat(getField(exitCode, "mExitParserPath"), is("/custom/pegasus-exitcode"));
        assertThat(getField(exitCode, "mExitCodeLogPath"), is("workflow.log"));
        assertThat(getField(exitCode, "mWFCacheMetadataLog"), is("workflow.cache.meta"));
        assertThat(getField(exitCode, "mPostScriptProperties"), is(""));
        assertThat(getField(exitCode, "mSubmitDir"), is("/submit/dir"));
    }

    @Test
    public void testConstructPopulatesDagmanProfiles() throws Exception {
        PegasusExitCode exitCode = new PegasusExitCode();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        String submitDir = "/submit/dir";
        String log = "workflow.log";
        exitCode.initialize(props, "/custom/pegasus-exitcode", submitDir, log);
        setField(exitCode, "mLogger", new NoOpLogManager());

        Job job = new Job();
        job.condorVariables.construct("output", submitDir + File.separator + "job.out");

        assertThat(exitCode.construct(job, Dagman.POST_SCRIPT_KEY), is(true));
        assertThat(job.dagmanVariables.get(Dagman.OUTPUT_KEY), is("./job.out"));
        assertThat(job.dagmanVariables.get(Dagman.POST_SCRIPT_KEY), is("/custom/pegasus-exitcode"));

        String arguments = (String) job.dagmanVariables.get(Dagman.POST_SCRIPT_ARGUMENTS_KEY);
        assertThat(
                arguments,
                allOf(
                        containsString(
                                PegasusExitCode
                                        .POSTSCRIPT_ARGUMENTS_FOR_PASSING_DAGMAN_JOB_EXITCODE),
                        containsString(" -l " + log),
                        containsString(" -M workflow.cache.meta")));
    }

    @Test
    public void testProtectedHelpersAndMethodSignatures() throws Exception {
        PegasusExitCode exitCode = new PegasusExitCode();

        Method getPostScriptProperties =
                PegasusExitCode.class.getDeclaredMethod(
                        "getPostScriptProperties", PegasusProperties.class);
        getPostScriptProperties.setAccessible(true);
        assertThat(
                getPostScriptProperties.invoke(exitCode, PegasusProperties.nonSingletonInstance()),
                is(""));

        Method appendProperty =
                PegasusExitCode.class.getDeclaredMethod(
                        "appendProperty", StringBuffer.class, String.class, String.class);
        appendProperty.setAccessible(true);
        StringBuffer sb = new StringBuffer();
        appendProperty.invoke(exitCode, sb, "pegasus.test.key", "value");
        assertThat(sb.toString(), is(" -Dpegasus.test.key=value"));

        assertMethod(
                "initialize",
                void.class,
                PegasusProperties.class,
                String.class,
                String.class,
                String.class);
        assertMethod("construct", boolean.class, Job.class, String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("getDefaultExitCodePath", String.class);

        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField("mProps", PegasusProperties.class, Modifier.PROTECTED);
        assertField("mExitParserPath", String.class, Modifier.PROTECTED);
        assertField("mDisablePerJobMetaFileCreation", boolean.class, Modifier.PROTECTED);
    }

    private Object getField(Object instance, String name) throws Exception {
        Field field = PegasusExitCode.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void setField(Object instance, String name, Object value) throws Exception {
        Field field = PegasusExitCode.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = PegasusExitCode.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusExitCode.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }

    private static class NoOpLogManager extends edu.isi.pegasus.common.logging.LogManager {

        @Override
        public void initialize(
                edu.isi.pegasus.common.logging.LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return System.out;
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
}
