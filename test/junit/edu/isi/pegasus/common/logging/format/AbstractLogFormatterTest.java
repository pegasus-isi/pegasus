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
package edu.isi.pegasus.common.logging.format;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.Event;
import edu.isi.pegasus.common.logging.LogFormatter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.Stack;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the AbstractLogFormatter class. */
public class AbstractLogFormatterTest {

    private Simple formatter;

    @BeforeEach
    public void setUp() {
        formatter = new Simple();
        formatter.setProgramName("pegasus");
    }

    // --- class structure ---

    @Test
    public void testImplementsLogFormatter() {
        assertThat(AbstractLogFormatter.class, typeCompatibleWith(LogFormatter.class));
    }

    @Test
    public void testKnownSubclasses() {
        assertThat(Netlogger.class, typeCompatibleWith(AbstractLogFormatter.class));
        assertThat(Simple.class, typeCompatibleWith(AbstractLogFormatter.class));
    }

    // --- setProgramName / getProgramName ---

    @Test
    public void testSetAndGetProgramName() {
        formatter.setProgramName("test-program");
        assertThat(formatter.getProgramName("anything"), is("test-program"));
    }

    @Test
    public void testGetProgramNameIgnoresArgument() {
        formatter.setProgramName("myapp");
        assertThat(formatter.getProgramName("other"), is("myapp"));
        assertThat(formatter.getProgramName(null), is("myapp"));
    }

    // --- addEvent / getEventName ---

    @Test
    public void testGetEventNameCurrentlyReturnsNullForSimpleEvent() {
        formatter.addEvent("event.first", "workflow", "wf-001");
        assertThat(formatter.getEventName(), is(nullValue()));
    }

    // --- getStartEventMessage / getEndEventMessage ---

    @Test
    public void testGetStartEventMessageContainsEventName() {
        formatter.addEvent("event.plan", "workflow", "wf-001");
        assertThat(formatter.getStartEventMessage(), containsString("event.plan"));
    }

    @Test
    public void testGetEndEventMessageContainsEventName() {
        formatter.addEvent("event.plan", "workflow", "wf-001");
        formatter.getStartEventMessage();
        assertThat(formatter.getEndEventMessage(), containsString("event.plan"));
    }

    // --- add(key, value) and createLogMessage ---

    @Test
    public void testAddKeyValueAndCreateLogMessage() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        formatter.add("job.id", "job-42");
        String msg = formatter.createLogMessage();
        assertThat(msg, containsString("job-42"));
    }

    @Test
    public void testAddWithDefaultKeyAndCreateLogMessage() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        formatter.add("hello world");
        String msg = formatter.createLogMessage();
        assertThat(msg, containsString("hello world"));
    }

    @Test
    public void testAddReturnsSelfForChaining() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        LogFormatter result = formatter.add("key", "value");
        assertThat(result, sameInstance(formatter));
    }

    // --- createLogMessageAndReset ---

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        formatter.add("key1", "value1");
        String first = formatter.createLogMessageAndReset();
        assertThat(first, containsString("value1"));

        formatter.add("key2", "value2");
        String second = formatter.createLogMessage();
        assertThat(second, not(containsString("value1")));
        assertThat(second, containsString("value2"));
    }

    // --- createEntityHierarchyMessage ---

    @Test
    public void testCreateEntityHierarchyMessage() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        String msg =
                formatter.createEntityHierarchyMessage(
                        "workflow", "wf-001", "job.id", Arrays.asList("job-a", "job-b"));
        assertThat(msg, containsString("wf-001"));
        assertThat(msg, containsString("job-a"));
        assertThat(msg, containsString("job-b"));
    }

    // --- stack behavior ---

    @Test
    public void testPopEventReturnsEvent() {
        formatter.addEvent("event.test", "workflow", "wf-001");
        Event e = formatter.popEvent();
        assertThat(e, is(notNullValue()));
    }

    @Test
    public void testNestedEventsStackOrder() {
        formatter.addEvent("event.outer", "workflow", "wf-001");
        formatter.addEvent("event.inner", "job", "job-001");
        assertThat(formatter.getEventName(), is(nullValue()));

        formatter.popEvent();
        assertThat(formatter.getEventName(), is(nullValue()));
    }

    @Test
    public void testPopEventOnEmptyStackThrows() {
        assertThrows(EmptyStackException.class, () -> formatter.popEvent());
    }

    @Test
    public void testGetEventNameOnEmptyStackThrows() {
        assertThrows(EmptyStackException.class, () -> formatter.getEventName());
    }

    @Test
    public void testAbstractLogFormatterIsAbstract() {
        assertThat(Modifier.isAbstract(AbstractLogFormatter.class.getModifiers()), is(true));
        assertThat(AbstractLogFormatter.class.isInterface(), is(false));
    }

    @Test
    public void testConstructorInitializesEmptyStack() throws Exception {
        Object value = ReflectionTestUtils.getField(new Simple(), "mStack");
        assertThat(value, instanceOf(Stack.class));
        assertThat(((Stack<?>) value).empty(), is(true));
    }

    @Test
    public void testAddEventMethodIsAbstract() throws Exception {
        Method method =
                AbstractLogFormatter.class.getDeclaredMethod(
                        "addEvent", String.class, String.class, String.class);
        assertThat(Modifier.isAbstract(method.getModifiers()), is(true));
    }

    @Test
    public void testDelegatedMessageMethodsThrowOnEmptyStack() {
        assertThrows(EmptyStackException.class, () -> formatter.getStartEventMessage());
        assertThrows(EmptyStackException.class, () -> formatter.getEndEventMessage());
        assertThrows(EmptyStackException.class, () -> formatter.createLogMessage());
        assertThrows(EmptyStackException.class, () -> formatter.createLogMessageAndReset());
    }
}
