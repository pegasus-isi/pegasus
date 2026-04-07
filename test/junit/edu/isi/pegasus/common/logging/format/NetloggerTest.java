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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Netlogger formatter class. */
public class NetloggerTest {

    private Netlogger mFormatter;

    @BeforeEach
    public void setUp() {
        mFormatter = new Netlogger();
        mFormatter.setProgramName("test-pegasus");
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testNetloggerIsConcreteClass() {
        assertThat(Modifier.isAbstract(Netlogger.class.getModifiers()), is(false));
    }

    @Test
    public void testNetloggerExtendsAbstractLogFormatter() {
        assertThat(AbstractLogFormatter.class.isAssignableFrom(Netlogger.class), is(true));
    }

    @Test
    public void testNetloggerImplementsLogFormatter() {
        assertThat(mFormatter, instanceOf(LogFormatter.class));
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetProgramNameIsReflectedInFormatter() {
        mFormatter.setProgramName("my-program");
        assertThat(mFormatter.getProgramName("any"), is("my-program"));
    }

    // -----------------------------------------------------------------------
    // addEvent(String, String, String)
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithThreeArgs() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-12345");
        String startMsg = mFormatter.getStartEventMessage();
        assertThat(startMsg, is(notNullValue()));
        assertThat(startMsg.isEmpty(), is(false));
    }

    @Test
    public void testAddEventWithThreeArgsEndMessage() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        String endMsg = mFormatter.getEndEventMessage();
        assertThat(endMsg, is(notNullValue()));
        assertThat(endMsg.isEmpty(), is(false));
    }

    // -----------------------------------------------------------------------
    // addEvent(String, Map)
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithMap() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-abc");
        entities.put("job", "job-001");
        mFormatter.addEvent("event.pegasus.plan", entities);
        String startMsg = mFormatter.getStartEventMessage();
        assertThat(startMsg, is(notNullValue()));
        assertThat(startMsg.isEmpty(), is(false));
    }

    @Test
    public void testAddEventWithMapEndMessage() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-002");
        mFormatter.addEvent("event.pegasus.map", entities);
        String endMsg = mFormatter.getEndEventMessage();
        assertThat(endMsg, is(notNullValue()));
        assertThat(endMsg.isEmpty(), is(false));
    }

    // -----------------------------------------------------------------------
    // add() and createLogMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueAppearsInLogMessage() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("job.id", "job-42");
        String logMsg = mFormatter.createLogMessage();
        assertThat(logMsg, is(notNullValue()));
        assertThat(logMsg, containsString("job-42"));
    }

    @Test
    public void testAddValueOnlyUsesDefaultKey() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("my-message-value");
        String logMsg = mFormatter.createLogMessage();
        assertThat(logMsg, is(notNullValue()));
        assertThat(logMsg, containsString("my-message-value"));
    }

    @Test
    public void testAddReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("k", "v");
        assertThat(result, is(sameInstance(mFormatter)));
    }

    @Test
    public void testChainedAddProducesAllValues() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("k1", "val-one").add("k2", "val-two").add("k3", "val-three");
        String logMsg = mFormatter.createLogMessage();
        assertThat(logMsg, containsString("val-one"));
        assertThat(logMsg, containsString("val-two"));
        assertThat(logMsg, containsString("val-three"));
    }

    // -----------------------------------------------------------------------
    // createLogMessageAndReset()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("temp.key", "temp-value");
        String firstMsg = mFormatter.createLogMessageAndReset();
        assertThat(firstMsg, containsString("temp-value"));
        String secondMsg = mFormatter.createLogMessage();
        assertThat(secondMsg, not(containsString("temp-value")));
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        // NetloggerEvent.getEventName() always returns null
        assertThat(mFormatter.getEventName(), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // Stack behaviour — popEvent()
    // -----------------------------------------------------------------------

    @Test
    public void testPopEventReturnsTopEvent() {
        mFormatter.addEvent("event.first", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertThat(popped, is(notNullValue()));
    }

    @Test
    public void testStackOperationsWithMultipleEvents() {
        mFormatter.addEvent("event.outer", "workflow", "wf-outer");
        mFormatter.add("outer.key", "outer-value");

        mFormatter.addEvent("event.inner", "workflow", "wf-inner");
        mFormatter.add("inner.key", "inner-value");

        // Top of stack is the inner event
        String innerMsg = mFormatter.createLogMessage();
        assertThat(innerMsg, containsString("inner-value"));
        assertThat(innerMsg, not(containsString("outer-value")));

        // Pop inner event; outer event becomes top
        mFormatter.popEvent();
        String outerMsg = mFormatter.createLogMessage();
        assertThat(outerMsg, containsString("outer-value"));
    }

    @Test
    public void testPopEventLeavesRemainingEventsOnStack() {
        mFormatter.addEvent("event.first", "workflow", "wf-001");
        mFormatter.addEvent("event.second", "workflow", "wf-002");
        mFormatter.popEvent();
        // Stack still has first event — getStartEventMessage should not throw
        assertDoesNotThrow(
                () -> mFormatter.getStartEventMessage(),
                "After popping second event, first event should still be accessible");
    }

    // -----------------------------------------------------------------------
    // createEntityHierarchyMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateEntityHierarchyMessageIsNotNull() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        List<String> children = Arrays.asList("job-1", "job-2", "job-3");
        String msg = mFormatter.createEntityHierarchyMessage("workflow", "wf-001", "job", children);
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-hier");
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-hier", "job", children);
        assertThat(msg, anyOf(containsString("job-alpha"), containsString("job-beta")));
    }

    @Test
    public void testAddEventPushesNetloggerEventInstance() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertThat(popped, instanceOf(NetloggerEvent.class));
    }

    @Test
    public void testThreeArgAddEventPropagatesProgramNameToEvent() {
        mFormatter.setProgramName("planner-a");
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");

        Event popped = mFormatter.popEvent();
        assertThat(popped.getProgramName("ignored"), is("planner-a"));
    }

    @Test
    public void testMapAddEventPropagatesProgramNameToEvent() {
        Netlogger formatter = new Netlogger();
        formatter.setProgramName("planner-b");
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-003");
        formatter.addEvent("event.pegasus.map", entities);

        Event popped = formatter.popEvent();
        assertThat(popped.getProgramName(null), is("planner-b"));
    }
}
