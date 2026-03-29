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
        assertFalse(
                Modifier.isAbstract(Netlogger.class.getModifiers()),
                "Netlogger should be a concrete class");
    }

    @Test
    public void testNetloggerExtendsAbstractLogFormatter() {
        assertTrue(
                AbstractLogFormatter.class.isAssignableFrom(Netlogger.class),
                "Netlogger should extend AbstractLogFormatter");
    }

    @Test
    public void testNetloggerImplementsLogFormatter() {
        assertTrue(mFormatter instanceof LogFormatter, "Netlogger should implement LogFormatter");
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetProgramNameIsReflectedInFormatter() {
        mFormatter.setProgramName("my-program");
        assertEquals(
                "my-program",
                mFormatter.getProgramName("any"),
                "Program name should be stored and returned");
    }

    // -----------------------------------------------------------------------
    // addEvent(String, String, String)
    // -----------------------------------------------------------------------

    @Test
    public void testAddEventWithThreeArgs() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-12345");
        String startMsg = mFormatter.getStartEventMessage();
        assertNotNull(startMsg, "getStartEventMessage should not be null after addEvent");
        assertFalse(startMsg.isEmpty(), "Start event message should not be empty");
    }

    @Test
    public void testAddEventWithThreeArgsEndMessage() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        String endMsg = mFormatter.getEndEventMessage();
        assertNotNull(endMsg, "getEndEventMessage should not return null");
        assertFalse(endMsg.isEmpty(), "End event message should not be empty");
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
        assertNotNull(startMsg, "getStartEventMessage should not be null after addEvent(map)");
        assertFalse(
                startMsg.isEmpty(),
                "Start event message from map-based addEvent should not be empty");
    }

    @Test
    public void testAddEventWithMapEndMessage() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-002");
        mFormatter.addEvent("event.pegasus.map", entities);
        String endMsg = mFormatter.getEndEventMessage();
        assertNotNull(endMsg, "getEndEventMessage should not return null for map-based event");
        assertFalse(endMsg.isEmpty(), "End event message from map-based event should not be empty");
    }

    // -----------------------------------------------------------------------
    // add() and createLogMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueAppearsInLogMessage() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("job.id", "job-42");
        String logMsg = mFormatter.createLogMessage();
        assertNotNull(logMsg, "createLogMessage should not return null");
        assertTrue(logMsg.contains("job-42"), "Log message should contain the added value");
    }

    @Test
    public void testAddValueOnlyUsesDefaultKey() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("my-message-value");
        String logMsg = mFormatter.createLogMessage();
        assertNotNull(logMsg, "createLogMessage should not return null after add(value)");
        assertTrue(
                logMsg.contains("my-message-value"),
                "Log message should contain value added via single-arg add()");
    }

    @Test
    public void testAddReturnsLogFormatterForChaining() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        LogFormatter result = mFormatter.add("k", "v");
        assertSame(mFormatter, result, "add(key,value) should return self-reference");
    }

    @Test
    public void testChainedAddProducesAllValues() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
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
    public void testCreateLogMessageAndResetClearsBuffer() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        mFormatter.add("temp.key", "temp-value");
        String firstMsg = mFormatter.createLogMessageAndReset();
        assertTrue(firstMsg.contains("temp-value"), "First message should contain 'temp-value'");
        String secondMsg = mFormatter.createLogMessage();
        assertFalse(
                secondMsg.contains("temp-value"),
                "After reset, subsequent message should not contain old value");
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-001");
        // NetloggerEvent.getEventName() always returns null
        assertNull(
                mFormatter.getEventName(),
                "getEventName() should return null per NetloggerEvent implementation");
    }

    // -----------------------------------------------------------------------
    // Stack behaviour — popEvent()
    // -----------------------------------------------------------------------

    @Test
    public void testPopEventReturnsTopEvent() {
        mFormatter.addEvent("event.first", "workflow", "wf-001");
        Event popped = mFormatter.popEvent();
        assertNotNull(popped, "popEvent() should return the event that was pushed");
    }

    @Test
    public void testStackOperationsWithMultipleEvents() {
        mFormatter.addEvent("event.outer", "workflow", "wf-outer");
        mFormatter.add("outer.key", "outer-value");

        mFormatter.addEvent("event.inner", "workflow", "wf-inner");
        mFormatter.add("inner.key", "inner-value");

        // Top of stack is the inner event
        String innerMsg = mFormatter.createLogMessage();
        assertTrue(
                innerMsg.contains("inner-value"), "Inner event message should contain inner value");
        assertFalse(
                innerMsg.contains("outer-value"),
                "Inner event message should not contain outer value");

        // Pop inner event; outer event becomes top
        mFormatter.popEvent();
        String outerMsg = mFormatter.createLogMessage();
        assertTrue(
                outerMsg.contains("outer-value"),
                "After pop, outer event message should contain outer value");
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
        assertNotNull(msg, "createEntityHierarchyMessage should not return null");
        assertFalse(msg.isEmpty(), "createEntityHierarchyMessage should not return empty string");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        mFormatter.addEvent("event.pegasus.plan", "workflow", "wf-hier");
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mFormatter.createEntityHierarchyMessage("workflow", "wf-hier", "job", children);
        assertTrue(
                msg.contains("job-alpha") || msg.contains("job-beta"),
                "Entity hierarchy message should reference at least one child ID");
    }
}
