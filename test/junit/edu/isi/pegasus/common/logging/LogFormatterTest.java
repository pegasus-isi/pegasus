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
package edu.isi.pegasus.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.format.Netlogger;
import edu.isi.pegasus.common.logging.format.Simple;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the LogFormatter interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class LogFormatterTest {

    // --- method declarations (NoSuchMethodException = test failure if missing) ---

    @Test
    public void testProgramNameMethods() throws NoSuchMethodException {
        LogFormatter.class.getMethod("setProgramName", String.class);
        LogFormatter.class.getMethod("getProgramName", String.class);
    }

    @Test
    public void testAddEventMethods() throws NoSuchMethodException {
        LogFormatter.class.getMethod("addEvent", String.class, String.class, String.class);
        LogFormatter.class.getMethod("addEvent", String.class, Map.class);
    }

    @Test
    public void testEventStackMethods() throws NoSuchMethodException {
        LogFormatter.class.getMethod("popEvent");
        LogFormatter.class.getMethod("getEventName");
        LogFormatter.class.getMethod("getStartEventMessage");
        LogFormatter.class.getMethod("getEndEventMessage");
    }

    @Test
    public void testAddMethods() throws NoSuchMethodException {
        LogFormatter.class.getMethod("add", String.class);
        LogFormatter.class.getMethod("add", String.class, String.class);
    }

    @Test
    public void testLogMessageMethods() throws NoSuchMethodException {
        LogFormatter.class.getMethod("createLogMessage");
        LogFormatter.class.getMethod("createLogMessageAndReset");
    }

    @Test
    public void testCreateEntityHierarchyMessageMethod() throws NoSuchMethodException {
        LogFormatter.class.getMethod(
                "createEntityHierarchyMessage",
                String.class,
                String.class,
                String.class,
                Collection.class);
    }

    // --- known implementors ---

    @Test
    public void testKnownImplementors() {
        assertThat(Netlogger.class, typeCompatibleWith(LogFormatter.class));
        assertThat(Simple.class, typeCompatibleWith(LogFormatter.class));
    }

    @Test
    public void testLogFormatterIsInterface() {
        assertThat(LogFormatter.class.isInterface(), is(true));
    }

    @Test
    public void testMethodReturnTypes() throws NoSuchMethodException {
        assertThat(
                LogFormatter.class.getMethod("setProgramName", String.class).getReturnType(),
                is(Void.TYPE));
        assertThat(
                LogFormatter.class.getMethod("getProgramName", String.class).getReturnType(),
                is(String.class));
        assertThat(
                LogFormatter.class
                        .getMethod("addEvent", String.class, String.class, String.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(
                LogFormatter.class.getMethod("addEvent", String.class, Map.class).getReturnType(),
                is(Void.TYPE));
        assertThat(LogFormatter.class.getMethod("popEvent").getReturnType(), is(Event.class));
        assertThat(LogFormatter.class.getMethod("getEventName").getReturnType(), is(String.class));
        assertThat(
                LogFormatter.class.getMethod("getStartEventMessage").getReturnType(),
                is(String.class));
        assertThat(
                LogFormatter.class.getMethod("getEndEventMessage").getReturnType(),
                is(String.class));
        assertThat(
                LogFormatter.class.getMethod("add", String.class).getReturnType(),
                is(LogFormatter.class));
        assertThat(
                LogFormatter.class.getMethod("add", String.class, String.class).getReturnType(),
                is(LogFormatter.class));
        assertThat(
                LogFormatter.class.getMethod("createLogMessage").getReturnType(), is(String.class));
        assertThat(
                LogFormatter.class.getMethod("createLogMessageAndReset").getReturnType(),
                is(String.class));
        assertThat(
                LogFormatter.class
                        .getMethod(
                                "createEntityHierarchyMessage",
                                String.class,
                                String.class,
                                String.class,
                                Collection.class)
                        .getReturnType(),
                is(String.class));
    }

    @Test
    public void testLogFormatterDeclaresExpectedNumberOfMethods() {
        assertThat(LogFormatter.class.getDeclaredMethods().length, is(13));
    }
}
