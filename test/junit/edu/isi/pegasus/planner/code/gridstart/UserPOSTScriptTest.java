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
public class UserPOSTScriptTest {

    @Test
    public void testShortNameAndImplementsPOSTScript() {
        assertThat(UserPOSTScript.SHORT_NAME, is("user"));
        assertThat(POSTScript.class.isAssignableFrom(UserPOSTScript.class), is(true));
    }

    @Test
    public void testInitializeSetsFieldsAndShortDescribe() throws Exception {
        UserPOSTScript postScript = new UserPOSTScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        postScript.initialize(props, "/custom/user-post.sh", "/submit/dir", "workflow.log");

        assertThat(postScript.shortDescribe(), is("user"));
        assertThat(getField(postScript, "mProps"), sameInstance(props));
        assertThat(getField(postScript, "mPOSTScriptPath"), is("/custom/user-post.sh"));
        assertThat(getField(postScript, "mLogger"), notNullValue());
    }

    @Test
    public void testInitializeRejectsNullPath() {
        UserPOSTScript postScript = new UserPOSTScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> postScript.initialize(props, null, "/submit/dir", "workflow.log"));

        assertThat(exception.getMessage(), is("Path to user specified postscript not given"));
    }

    @Test
    public void testConstructPopulatesDagmanProfiles() throws Exception {
        UserPOSTScript postScript = new UserPOSTScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        postScript.initialize(props, "/custom/user-post.sh", "/submit/dir", "workflow.log");
        setField(postScript, "mLogger", new NoOpLogManager());

        Job job = new Job();
        job.condorVariables.construct("output", "/submit/dir/job.out");

        assertThat(postScript.construct(job, Dagman.POST_SCRIPT_KEY), is(true));
        assertThat(job.dagmanVariables.get(Dagman.OUTPUT_KEY), is("/submit/dir/job.out"));
        assertThat(job.dagmanVariables.get(Dagman.POST_SCRIPT_KEY), is("/custom/user-post.sh"));
    }

    @Test
    public void testMethodAndFieldSignatures() throws Exception {
        assertMethod(
                "initialize",
                void.class,
                PegasusProperties.class,
                String.class,
                String.class,
                String.class);
        assertMethod("construct", boolean.class, Job.class, String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("getExitCodePath", String.class);

        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField("mProps", PegasusProperties.class, Modifier.PROTECTED);
        assertField("mPOSTScriptPath", String.class, Modifier.PROTECTED);
    }

    private Object getField(Object instance, String name) throws Exception {
        Field field = UserPOSTScript.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void setField(Object instance, String name, Object value) throws Exception {
        Field field = UserPOSTScript.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(instance, value);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = UserPOSTScript.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = UserPOSTScript.class.getMethod(name, parameterTypes);
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
