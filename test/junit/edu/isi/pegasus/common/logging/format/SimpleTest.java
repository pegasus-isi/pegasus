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
        assertFalse(
                Modifier.isAbstract(Simple.class.getModifiers()),
                "Simple should be a concrete class");
    }

    @Test
    public void testSimpleExtendsAbstractLogFormatter() {
        assertTrue(
                AbstractLogFormatter.class.isAssignableFrom(Simple.class),
                "Simple should extend AbstractLogFormatter");
    }

    @Test
    public void testSimpleImplementsLogFormatter() {
        assertTrue(mFormatter instanceof LogFormatter, "Simple should implement LogFormatter");
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetProgramNameStored() {
        mFormatter.setProgramName("new-program");
        assertEquals(
                "new-program",
                mFormatter.getProgramName("ignored"),
                "getProgramName should return the set program name");
    }

    // -----------------------------------------------------------------------
    // addEvent(String, String, String) — backed by SimpleEvent
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithThreeArgs() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-99");
        String startMsg = mFormatter.getStartEventMessage();
        assertNotNull(startMsg, "getStartEventMessage should not return null");
        assertTrue(startMsg.contains("STARTED"), "Simple start message should contain 'STARTED'");
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEventName() {
        mFormatter.addEvent("event.my.name", "workflow", "wf-001");
        String msg = mFormatter.getStartEventMessage();
        assertTrue(msg.contains("event.my.name"), "Start message should contain the event name");
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEntityName() {
        mFormatter.addEvent("event.test", "workflow", "wf-ent");
        String msg = mFormatter.getStartEventMessage();
        assertTrue(msg.contains("workflow"), "Start message should contain the entity name");
    }

    @Test
    public void testAddEventThreeArgsStartMessageContainsEntityID() {
        mFormatter.addEvent("event.test", "workflow", "wf-id-42");
        String msg = mFormatter.getStartEventMessage();
        assertTrue(msg.contains("wf-id-42"), "Start message should contain the entity ID");
    }

    @Test
    public void testEndMessageContainsFinished() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.getStartEventMessage();
        String endMsg = mFormatter.getEndEventMessage();
        assertNotNull(endMsg, "End message should not be null");
        assertTrue(endMsg.contains("FINISHED"), "Simple end message should contain 'FINISHED'");
    }

    @Test
    public void testEndMessageContainsSeconds() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-dur");
        mFormatter.getStartEventMessage();
        String endMsg = mFormatter.getEndEventMessage();
        assertTrue(
                endMsg.contains("seconds"),
                "Simple end message should include a duration in seconds");
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
        assertNotNull(startMsg, "Start message from map event should not be null");
        assertFalse(startMsg.isEmpty(), "Start message from map event should not be empty");
    }

    @Test
    public void testAddEventWithMapEndMessageNotNull() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-end");
        mFormatter.addEvent("event.map.end", entities);
        String endMsg = mFormatter.getEndEventMessage();
        assertNotNull(endMsg, "End message from map event should not be null");
        assertFalse(endMsg.isEmpty(), "End message from map event should not be empty");
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
        assertFalse(
                startMsg.contains("STARTED"),
                "Map-based addEvent uses NetloggerEvent whose start message does not contain 'STARTED'");
    }

    // -----------------------------------------------------------------------
    // add(String value) — single-arg, passes empty key to SimpleEvent
    // -----------------------------------------------------------------------

    @Test
    public void testAddValueOnlyAppearsInLog() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("just a message");
        String logMsg = mFormatter.createLogMessage();
        assertNotNull(logMsg, "Log message should not be null");
        assertTrue(
                logMsg.contains("just a message"),
                "Log message should contain the plain string added");
    }

    @Test
    public void testAddValueOnlyReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("some value");
        assertSame(mFormatter, result, "add(value) should return self-reference");
    }

    // -----------------------------------------------------------------------
    // add(String key, String value) — key+value pair
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueBothAppearInLogMessage() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("job.id", "job-77");
        String logMsg = mFormatter.createLogMessage();
        assertTrue(logMsg.contains("job.id"), "Log message should contain the key");
        assertTrue(logMsg.contains("job-77"), "Log message should contain the value");
    }

    @Test
    public void testAddKeyValueReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("k", "v");
        assertSame(mFormatter, result, "add(key, value) should return self-reference");
    }

    @Test
    public void testChainedAddAccumulatesAllValues() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("k1", "val-one").add("k2", "val-two").add("k3", "val-three");
        String logMsg = mFormatter.createLogMessage();
        assertTrue(logMsg.contains("val-one"), "Chained log message should contain val-one");
        assertTrue(logMsg.contains("val-two"), "Chained log message should contain val-two");
        assertTrue(logMsg.contains("val-three"), "Chained log message should contain val-three");
    }

    // -----------------------------------------------------------------------
    // createLogMessageAndReset()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateLogMessageAndResetReturnsAccumulatedContent() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("temp.key", "temp-value");
        String msg = mFormatter.createLogMessageAndReset();
        assertTrue(
                msg.contains("temp-value"),
                "createLogMessageAndReset should return the buffered content");
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        mFormatter.add("k", "stale-value");
        mFormatter.createLogMessageAndReset();
        String second = mFormatter.createLogMessage();
        assertFalse(
                second.contains("stale-value"),
                "After createLogMessageAndReset, subsequent message should not contain old value");
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-001");
        // SimpleEvent.getEventName() always returns null
        assertNull(
                mFormatter.getEventName(),
                "getEventName() should return null per the SimpleEvent implementation");
    }

    // -----------------------------------------------------------------------
    // Stack behaviour — popEvent()
    // -----------------------------------------------------------------------

    @Test
    public void testPopEventReturnsTopEvent() {
        mFormatter.addEvent("event.only", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertNotNull(popped, "popEvent() should return the event that was pushed");
    }

    @Test
    public void testStackIsLIFO() {
        mFormatter.addEvent("event.outer", "workflow", "wf-outer");
        mFormatter.add("outer.key", "outer-value");

        mFormatter.addEvent("event.inner", "workflow", "wf-inner");
        mFormatter.add("inner.key", "inner-value");

        // Top of stack is the inner event
        String innerMsg = mFormatter.createLogMessage();
        assertTrue(
                innerMsg.contains("inner-value"), "Inner event message should contain inner value");
        assertFalse(innerMsg.contains("outer-value"), "Inner event should not expose outer value");

        // Pop inner; outer becomes active
        mFormatter.popEvent();
        String outerMsg = mFormatter.createLogMessage();
        assertTrue(
                outerMsg.contains("outer-value"),
                "After pop, outer event message should be active");
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
        assertNotNull(msg, "createEntityHierarchyMessage should not return null");
        assertFalse(msg.isEmpty(), "createEntityHierarchyMessage should not be empty");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsParentID() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-parent");
        List<String> children = Arrays.asList("job-a");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-parent", "job", children);
        assertTrue(msg.contains("wf-parent"), "Hierarchy message should contain the parent ID");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        mFormatter.addEvent("event.simple.test", "workflow", "wf-hier");
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-hier", "job", children);
        assertTrue(msg.contains("job-alpha"), "Hierarchy message should contain job-alpha");
        assertTrue(msg.contains("job-beta"), "Hierarchy message should contain job-beta");
    }
}
