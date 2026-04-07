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
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class NetloggerPostScriptTest {

    @Test
    public void testConstantsAndImplementsPOSTScript() throws Exception {
        assertThat(NetloggerPostScript.SHORT_NAME, is("Netlogger"));
        assertThat(NetloggerPostScript.WORKFLOW_ID_PROPERTY, is("pegasus.gridstart.workflow.id"));
        assertThat(POSTScript.class.isAssignableFrom(NetloggerPostScript.class), is(true));

        Field field = NetloggerPostScript.class.getDeclaredField("LOG4J_CONF_PROPERTY");
        field.setAccessible(true);
        assertThat(field.get(null), is("log4j.configuration"));
    }

    @Test
    public void testInitializeSetsExpectedFieldsAndShortDescribe() throws Exception {
        NetloggerPostScript postScript = new NetloggerPostScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(NetloggerPostScript.WORKFLOW_ID_PROPERTY, "wf-123");

        postScript.initialize(props, "/custom/netlogger-exitcode", "/submit/dir", "workflow.log");

        assertThat(postScript.shortDescribe(), is("Netlogger"));
        assertThat(getField(postScript, "mProps"), sameInstance(props));
        assertThat(getField(postScript, "mPOSTScriptPath"), is("/custom/netlogger-exitcode"));
        assertThat(getField(postScript, "mWorkflowID"), is("wf-123"));
        assertThat(getField(postScript, "mLogger"), notNullValue());
    }

    @Test
    public void testConstructPopulatesDagmanProfiles() throws Exception {
        NetloggerPostScript postScript = new NetloggerPostScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(NetloggerPostScript.WORKFLOW_ID_PROPERTY, "workflow-1");
        postScript.initialize(props, "/custom/netlogger-exitcode", "/submit/dir", "workflow.log");
        setField(postScript, "mLogger", new NoOpLogManager());

        Job job = new Job();
        job.jobName = "jobA";
        job.condorVariables.construct("output", "/submit/dir/jobA.out");

        assertThat(postScript.construct(job, Dagman.POST_SCRIPT_KEY), is(true));
        assertThat(job.dagmanVariables.get(Dagman.OUTPUT_KEY), is("/submit/dir/jobA.out"));
        assertThat(
                job.dagmanVariables.get(Dagman.POST_SCRIPT_KEY), is("/custom/netlogger-exitcode"));

        String arguments = (String) job.dagmanVariables.get(Dagman.POST_SCRIPT_ARGUMENTS_KEY);
        assertThat(
                arguments,
                allOf(
                        containsString(" -j jobA -w workflow-1 -f "),
                        containsString("-Dpegasus.user.properties=")));
    }

    @Test
    public void testHelperMethodsAndFieldShapes() throws Exception {
        NetloggerPostScript postScript = new NetloggerPostScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        Method appendProperty =
                NetloggerPostScript.class.getDeclaredMethod(
                        "appendProperty", StringBuffer.class, String.class, String.class);
        appendProperty.setAccessible(true);
        StringBuffer sb = new StringBuffer();
        appendProperty.invoke(postScript, sb, "pegasus.test.key", "value");
        assertThat(sb.toString(), is(" -Dpegasus.test.key=value"));

        Method getPostScriptProperties =
                NetloggerPostScript.class.getDeclaredMethod(
                        "getPostScriptProperties", PegasusProperties.class);
        getPostScriptProperties.setAccessible(true);
        String propertyArgs = (String) getPostScriptProperties.invoke(postScript, props);
        assertThat(propertyArgs, startsWith(" -Dpegasus.user.properties="));

        assertMethod(
                "initialize",
                void.class,
                PegasusProperties.class,
                String.class,
                String.class,
                String.class);
        assertMethod("construct", boolean.class, Job.class, String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("getNetloggerExitCodePath", String.class);

        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField("mProps", PegasusProperties.class, Modifier.PROTECTED);
        assertField("mPOSTScriptPath", String.class, Modifier.PROTECTED);
    }

    private Object getField(Object instance, String name) throws Exception {
        Field field = NetloggerPostScript.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void setField(Object instance, String name, Object value) throws Exception {
        Field field = NetloggerPostScript.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = NetloggerPostScript.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = NetloggerPostScript.class.getMethod(name, parameterTypes);
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
