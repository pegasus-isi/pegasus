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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class NoPOSTScriptTest {

    @Test
    public void testShortNameAndImplementsPOSTScript() {
        assertThat(NoPOSTScript.SHORT_NAME, is("none"));
        assertThat(POSTScript.class.isAssignableFrom(NoPOSTScript.class), is(true));
    }

    @Test
    public void testInitializeAndShortDescribe() throws Exception {
        NoPOSTScript postScript = new NoPOSTScript();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        postScript.initialize(props, "/ignored", "/submit", "workflow.log");

        assertThat(postScript.shortDescribe(), is("none"));
        assertThat(getField(postScript, "mProps"), sameInstance(props));
        assertThat(getField(postScript, "mLogger"), notNullValue());
    }

    @Test
    public void testConstructRemovesPostScriptProfilesAndReturnsFalse() {
        NoPOSTScript postScript = new NoPOSTScript();
        Job job = new Job();
        job.dagmanVariables.construct(Dagman.POST_SCRIPT_KEY, "/path/to/postscript");
        job.dagmanVariables.construct(Dagman.POST_SCRIPT_ARGUMENTS_KEY, "--some-args");

        boolean result = postScript.construct(job, Dagman.POST_SCRIPT_KEY);

        assertThat(result, is(false));
        assertThat(job.dagmanVariables.get(Dagman.POST_SCRIPT_KEY), nullValue());
        assertThat(job.dagmanVariables.get(Dagman.POST_SCRIPT_ARGUMENTS_KEY), nullValue());
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
        assertMethod("shortDescribe", String.class);
        assertMethod("construct", boolean.class, Job.class, String.class);

        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField("mProps", PegasusProperties.class, Modifier.PROTECTED);
    }

    private Object getField(Object instance, String name) throws Exception {
        Field field = NoPOSTScript.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = NoPOSTScript.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = NoPOSTScript.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }
}
