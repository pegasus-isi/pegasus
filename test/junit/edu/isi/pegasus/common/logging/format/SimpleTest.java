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

/** Tests for the Simple log formatter class. */
public class SimpleTest {

    private Simple mFormatter;

    @BeforeEach
    public void setUp() {
        mFormatter = new Simple();
        mFormatter.setProgramName("test-pegasus");
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testSimpleIsConcreteClass() {
        assertThat(Modifier.isAbstract(Simple.class.getModifiers()), is(false));
    }

    @Test
    public void testSimpleExtendsAbstractLogFormatter() {
        assertThat(AbstractLogFormatter.class.isAssignableFrom(Simple.class), is(true));
    }

    @Test
    public void testSimpleImplementsLogFormatter() {
        assertThat(mFormatter, instanceOf(LogFormatter.class));
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetProgramNameStored() {
        mFormatter.setProgramName("new-program");
        assertThat(mFormatter.getProgramName("ignored"), is("new-program"));
    }

    // -----------------------------------------------------------------------
    // addEvent(String, String, String) — backed by SimpleEvent
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithThreeArgs() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-99");
        String startMsg = mFormatter.getStartEventMessage();
        assertThat(startMsg, is(notNullValue()));
        assertThat(startMsg, containsString("STARTED"));
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEventName() {
        mFormatter.addEvent("event.my.name", "workflow", "wf-001");
        String msg = mFormatter.getStartEventMessage();
        assertThat(msg, containsString("event.my.name"));
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEntityName() {
        mFormatter.addEvent("event.test", "workflow", "wf-ent");
        String msg = mFormatter.getStartEventMessage();
        assertThat(msg, containsString("workflow"));
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEntityID() {
        mFormatter.addEvent("event.test", "workflow", "wf-id-42");
        String msg = mFormatter.getStartEventMessage();
        assertThat(msg, containsString("wf-id-42"));
    }

    @Test
    public void testEndMessageContainsFinished() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.getStartEventMessage();
        String endMsg = mFormatter.getEndEventMessage();
        assertThat(endMsg, is(notNullValue()));
        assertThat(endMsg, containsString("FINISHED"));
    }

    @Test
    public void testEndMessageContainsSeconds() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-dur");
        mFormatter.getStartEventMessage();
        String endMsg = mFormatter.getEndEventMessage();
        assertThat(endMsg, containsString("seconds"));
    }

    // -----------------------------------------------------------------------
    // addEvent(String, Map) — backed by NetloggerEvent
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithMap() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map");
        mFormatter.addEvent("event.simple.map", entities);
        String startMsg = mFormatter.getStartEventMessage();
        assertThat(startMsg, is(notNullValue()));
        assertThat(startMsg.isEmpty(), is(false));
    }

    @Test
    public void testAddEventWithMapEndMessageNotNull() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-end");
        mFormatter.addEvent("event.map.end", entities);
        String endMsg = mFormatter.getEndEventMessage();
        assertThat(endMsg, is(notNullValue()));
        assertThat(endMsg.isEmpty(), is(false));
    }

    /**
     * addEvent(String, Map) intentionally pushes a NetloggerEvent, not a SimpleEvent. The start
     * message therefore does NOT contain "STARTED" (that is SimpleEvent-specific formatting).
     */
    @Test
    public void testAddEventWithMapUsesNetloggerEventNotSimpleEvent() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-nl");
        mFormatter.addEvent("event.map.nl", entities);
        String startMsg = mFormatter.getStartEventMessage();
        assertThat(startMsg, not(containsString("STARTED")));
    }

    // -----------------------------------------------------------------------
    // add(String value) — single-arg, passes empty key to SimpleEvent
    // -----------------------------------------------------------------------

    @Test
    public void testAddValueOnlyAppearsInLog() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("just a message");
        String logMsg = mFormatter.createLogMessage();
        assertThat(logMsg, is(notNullValue()));
        assertThat(logMsg, containsString("just a message"));
    }

    @Test
    public void testAddValueOnlyReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("some value");
        assertThat(result, is(sameInstance(mFormatter)));
    }

    // -----------------------------------------------------------------------
    // add(String key, String value) — key+value pair
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueBothAppearInLogMessage() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("job.id", "job-77");
        String logMsg = mFormatter.createLogMessage();
        assertThat(logMsg, containsString("job.id"));
        assertThat(logMsg, containsString("job-77"));
    }

    @Test
    public void testAddKeyValueReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("k", "v");
        assertThat(result, is(sameInstance(mFormatter)));
    }

    @Test
    public void testChainedAddAccumulatesAllValues() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
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
    public void testCreateLogMessageAndResetReturnsAccumulatedContent() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("temp.key", "temp-value");
        String msg = mFormatter.createLogMessageAndReset();
        assertThat(msg, containsString("temp-value"));
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("k", "stale-value");
        mFormatter.createLogMessageAndReset();
        String second = mFormatter.createLogMessage();
        assertThat(second, not(containsString("stale-value")));
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        // SimpleEvent.getEventName() always returns null
        assertThat(mFormatter.getEventName(), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // Stack behaviour — popEvent()
    // -----------------------------------------------------------------------

    @Test
    public void testPopEventReturnsTopEvent() {
        mFormatter.addEvent("event.only", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertThat(popped, is(notNullValue()));
    }

    @Test
    public void testStackIsLIFO() {
        mFormatter.addEvent("event.outer", "workflow", "wf-outer");
        mFormatter.add("outer.key", "outer-value");

        mFormatter.addEvent("event.inner", "workflow", "wf-inner");
        mFormatter.add("inner.key", "inner-value");

        // Top of stack is the inner event
        String innerMsg = mFormatter.createLogMessage();
        assertThat(innerMsg, containsString("inner-value"));
        assertThat(innerMsg, not(containsString("outer-value")));

        // Pop inner; outer becomes active
        mFormatter.popEvent();
        String outerMsg = mFormatter.createLogMessage();
        assertThat(outerMsg, containsString("outer-value"));
    }

    @Test
    public void testPopEventLeavesRemainingEventsOnStack() {
        mFormatter.addEvent("event.first", "workflow", "wf-001");
        mFormatter.addEvent("event.second", "workflow", "wf-002");
        mFormatter.popEvent();
        assertDoesNotThrow(
                () -> mFormatter.getStartEventMessage(),
                "After popping second event, first event should still be accessible");
    }

    // -----------------------------------------------------------------------
    // createEntityHierarchyMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateEntityHierarchyMessageIsNotNull() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        List<String> children = Arrays.asList("job-1", "job-2");
        String msg = mFormatter.createEntityHierarchyMessage("workflow", "wf-001", "job", children);
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsParentID() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-parent");
        List<String> children = Arrays.asList("job-a");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-parent", "job", children);
        assertThat(msg, containsString("wf-parent"));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-hier");
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-hier", "job", children);
        assertThat(msg, containsString("job-alpha"));
        assertThat(msg, containsString("job-beta"));
    }

    @Test
    public void testThreeArgAddEventPushesSimpleEvent() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertThat(popped, instanceOf(SimpleEvent.class));
    }

    @Test
    public void testThreeArgAddEventPropagatesProgramName() {
        mFormatter.setProgramName("planner-simple");
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");

        Event popped = mFormatter.popEvent();
        assertThat(popped.getProgramName("ignored"), is("planner-simple"));
    }

    @Test
    public void testMapAddEventPushesNetloggerEventAndPropagatesProgramName() {
        mFormatter.setProgramName("planner-map");
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-prog");
        mFormatter.addEvent("event.simple.map", entities);

        Event popped = mFormatter.popEvent();
        assertThat(popped, instanceOf(NetloggerEvent.class));
        assertThat(popped.getProgramName(null), is("planner-map"));
    }
}
