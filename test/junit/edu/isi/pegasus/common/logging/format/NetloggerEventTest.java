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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the NetloggerEvent class. */
public class NetloggerEventTest {

    private NetloggerEvent mEvent;

    @BeforeEach
    public void setUp() {
        mEvent = new NetloggerEvent();
        mEvent.setProgramName("test-pegasus");
        mEvent.setEvent("event.pegasus.plan", "workflow", "wf-001");
    }

    @Test
    public void testNetloggerEventIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(NetloggerEvent.class.getModifiers()),
                "NetloggerEvent should be a concrete class");
    }

    @Test
    public void testNetloggerEventImplementsEvent() {
        assertTrue(mEvent instanceof Event, "NetloggerEvent should implement Event");
    }

    @Test
    public void testSetAndGetProgramName() {
        mEvent.setProgramName("my-prog");
        assertEquals("my-prog", mEvent.getProgramName("ignored"), "Program name should be stored");
    }

    @Test
    public void testGetStartEventMessageIsNotNull() {
        String msg = mEvent.getStartEventMessage();
        assertNotNull(msg, "getStartEventMessage should not return null");
        assertFalse(msg.isEmpty(), "Start event message should not be empty");
    }

    @Test
    public void testGetEndEventMessageIsNotNull() {
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertNotNull(msg, "getEndEventMessage should not return null");
        assertFalse(msg.isEmpty(), "End event message should not be empty");
    }

    @Test
    public void testAddKeyValueInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertNotNull(logMsg, "createLogMessage should not return null");
        assertTrue(logMsg.contains("job-99"), "Log message should contain the added value");
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mEvent.add("key", "value1");
        String firstMsg = mEvent.createLogMessageAndReset();
        assertTrue(firstMsg.contains("value1"), "First message should contain 'value1'");
        // After reset, value1 should not be in next message
        String secondMsg = mEvent.createLogMessage();
        assertFalse(
                secondMsg.contains("value1"), "After reset, message should not contain old value");
    }

    @Test
    public void testAddReturnsEventForChaining() {
        Event result = mEvent.add("key", "val");
        assertSame(mEvent, result, "add() should return self-reference for chaining");
    }

    @Test
    public void testSetEventWithMapCreatesValidStartMessage() {
        NetloggerEvent event = new NetloggerEvent();
        event.setProgramName("test-pegasus");
        Map<String, String> entities = new HashMap<>();
        entities.put("workflow", "wf-map-001");
        entities.put("job", "job-map-001");
        event.setEvent("event.pegasus.map", entities);
        String msg = event.getStartEventMessage();
        assertNotNull(msg, "Start message with map-based event should not be null");
        assertFalse(msg.isEmpty(), "Start message with map-based event should not be empty");
    }

    @Test
    public void testSetEventWithMapCreatesValidEndMessage() {
        NetloggerEvent event = new NetloggerEvent();
        event.setProgramName("test-pegasus");
        Map<String, String> entities = new HashMap<>();
        entities.put("workflow", "wf-map-002");
        event.setEvent("event.pegasus.map", entities);
        String msg = event.getEndEventMessage();
        assertNotNull(msg, "End message with map-based event should not be null");
        assertFalse(msg.isEmpty(), "End message with map-based event should not be empty");
    }

    @Test
    public void testSetEventWithMapResetsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        Map<String, String> entities = new HashMap<>();
        entities.put("workflow", "wf-reset-001");
        mEvent.setEvent("event.pegasus.reset", entities);
        String msg = mEvent.createLogMessage();
        assertFalse(
                msg.contains("stale-value"),
                "setEvent with map should reset the internal log buffer");
    }

    @Test
    public void testSetEventWithStringResetsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        mEvent.setEvent("event.pegasus.plan2", "workflow", "wf-002");
        String msg = mEvent.createLogMessage();
        assertFalse(
                msg.contains("stale-value"),
                "setEvent with string args should reset the internal log buffer");
    }

    @Test
    public void testGetEventNameReturnsNull() {
        assertNull(
                mEvent.getEventName(), "getEventName() should return null per the implementation");
    }

    @Test
    public void testChainedAddProducesAllValues() {
        mEvent.add("k1", "v1").add("k2", "v2").add("k3", "v3");
        String logMsg = mEvent.createLogMessage();
        assertTrue(logMsg.contains("v1"), "Chained log message should contain v1");
        assertTrue(logMsg.contains("v2"), "Chained log message should contain v2");
        assertTrue(logMsg.contains("v3"), "Chained log message should contain v3");
    }

    @Test
    public void testCreateEntityHierarchyMessageIsNotNull() {
        List<String> children = Arrays.asList("job-1", "job-2", "job-3");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-001", "job", children);
        assertNotNull(msg, "createEntityHierarchyMessage should not return null");
        assertFalse(msg.isEmpty(), "createEntityHierarchyMessage should not be empty");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mEvent.createEntityHierarchyMessage("workflow", "wf-hier-001", "job", children);
        assertTrue(
                msg.contains("job-alpha") || msg.contains("job-beta"),
                "Entity hierarchy message should reference child IDs");
    }

    @Test
    public void testResetProducesCleanLogMessage() {
        mEvent.add("before.reset", "dirty-value");
        mEvent.reset();
        String msg = mEvent.createLogMessage();
        assertFalse(
                msg.contains("dirty-value"),
                "After explicit reset(), dirty value should not appear in log message");
    }
}
