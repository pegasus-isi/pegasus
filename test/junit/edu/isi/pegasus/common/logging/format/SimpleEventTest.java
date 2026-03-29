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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the SimpleEvent class. */
public class SimpleEventTest {

    private SimpleEvent mEvent;

    @BeforeEach
    public void setUp() {
        mEvent = new SimpleEvent();
        mEvent.setProgramName("test-pegasus");
        mEvent.setEvent("event.test", "workflow", "wf-001");
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testSimpleEventImplementsEvent() {
        assertTrue(mEvent instanceof Event, "SimpleEvent should implement Event");
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetProgramName() {
        mEvent.setProgramName("my-program");
        assertEquals(
                "my-program", mEvent.getProgramName("ignored"), "Program name should be stored");
    }

    // -----------------------------------------------------------------------
    // setEvent(String, String, String)
    // -----------------------------------------------------------------------

    @Test
    public void testGetStartEventMessageContainsStarted() {
        String msg = mEvent.getStartEventMessage();
        assertNotNull(msg, "getStartEventMessage should not return null");
        assertTrue(msg.contains("STARTED"), "Start message should contain 'STARTED'");
    }

    @Test
    public void testStartMessageContainsEventName() {
        mEvent.setEvent("my.event.name", "wf", "wf-42");
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("my.event.name"), "Start message should contain the event name");
    }

    @Test
    public void testStartMessageContainsEntityName() {
        mEvent.setEvent("event.test", "workflow", "wf-ent");
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("workflow"), "Start message should contain the entity name");
    }

    @Test
    public void testStartMessageContainsEntityID() {
        mEvent.setEvent("event.test", "workflow", "wf-id-999");
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("wf-id-999"), "Start message should contain the entity ID");
    }

    @Test
    public void testSetEventOverwritesPreviousEvent() {
        mEvent.setEvent("event.first", "workflow", "wf-first");
        mEvent.setEvent("event.second", "workflow", "wf-second");
        String msg = mEvent.getStartEventMessage();
        assertTrue(
                msg.contains("event.second"),
                "Start message should reflect the latest setEvent call");
        assertFalse(
                msg.contains("event.first"), "Start message should not contain stale event name");
    }

    // -----------------------------------------------------------------------
    // setEvent(String, Map)
    // -----------------------------------------------------------------------

    @Test
    public void testSetEventWithMapSetsEventName() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-abc");
        mEvent.setEvent("event.from.map", entities);
        String msg = mEvent.getStartEventMessage();
        assertTrue(
                msg.contains("event.from.map"),
                "Start message should contain the event name set via map");
    }

    @Test
    public void testSetEventWithMapContainsEntityKey() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-001");
        mEvent.setEvent("event.map.test", entities);
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("workflow"), "Start message should contain map entity key");
    }

    @Test
    public void testSetEventWithMapContainsEntityValue() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-002");
        mEvent.setEvent("event.map.test", entities);
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("wf-map-002"), "Start message should contain map entity value");
    }

    @Test
    public void testSetEventWithMapMultipleEntries() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-multi");
        entities.put("job", "job-multi");
        mEvent.setEvent("event.multi", entities);
        String msg = mEvent.getStartEventMessage();
        assertTrue(msg.contains("wf-multi"), "Start message should contain first entity value");
        assertTrue(msg.contains("job-multi"), "Start message should contain second entity value");
    }

    // -----------------------------------------------------------------------
    // getEndEventMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEndEventMessageContainsFinished() {
        mEvent.getStartEventMessage(); // set mStart
        String msg = mEvent.getEndEventMessage();
        assertNotNull(msg, "getEndEventMessage should not return null");
        assertTrue(msg.contains("FINISHED"), "End message should contain 'FINISHED'");
    }

    @Test
    public void testGetEndEventMessageContainsSeconds() {
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertTrue(msg.contains("seconds"), "End message should contain duration in seconds");
    }

    @Test
    public void testGetEndEventMessageContainsEventName() {
        mEvent.setEvent("event.duration", "workflow", "wf-dur");
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertTrue(msg.contains("event.duration"), "End message should contain the event name");
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        assertNull(
                mEvent.getEventName(), "getEventName() should return null per the implementation");
    }

    // -----------------------------------------------------------------------
    // add() and createLogMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueAppearsInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertTrue(logMsg.contains("job-99"), "Log message should contain the value added");
    }

    @Test
    public void testAddKeyAppearsInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertTrue(logMsg.contains("job.id"), "Log message should contain the key added");
    }

    @Test
    public void testAddReturnsEventForChaining() {
        Event result = mEvent.add("k", "v");
        assertSame(mEvent, result, "add() should return self-reference for method chaining");
    }

    @Test
    public void testChainedAddAccumulatesAllValues() {
        mEvent.add("k1", "val-one").add("k2", "val-two").add("k3", "val-three");
        String logMsg = mEvent.createLogMessage();
        assertTrue(logMsg.contains("val-one"), "Chained log message should contain val-one");
        assertTrue(logMsg.contains("val-two"), "Chained log message should contain val-two");
        assertTrue(logMsg.contains("val-three"), "Chained log message should contain val-three");
    }

    @Test
    public void testCreateLogMessageIsNotNull() {
        String logMsg = mEvent.createLogMessage();
        assertNotNull(logMsg, "createLogMessage() should not return null");
    }

    // -----------------------------------------------------------------------
    // createLogMessageAndReset()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateLogMessageAndResetReturnsAccumulatedContent() {
        mEvent.add("k", "v1");
        String msg = mEvent.createLogMessageAndReset();
        assertTrue(
                msg.contains("v1"), "createLogMessageAndReset should return the buffered content");
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mEvent.add("k", "v1");
        String first = mEvent.createLogMessageAndReset();
        assertTrue(first.contains("v1"), "First message should contain 'v1'");
        String second = mEvent.createLogMessage();
        assertFalse(second.contains("v1"), "After reset, message should not contain old value");
    }

    // -----------------------------------------------------------------------
    // reset()
    // -----------------------------------------------------------------------

    @Test
    public void testExplicitResetClearsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        mEvent.reset();
        String msg = mEvent.createLogMessage();
        assertFalse(
                msg.contains("stale-value"),
                "After explicit reset(), dirty log buffer value should not appear");
    }

    @Test
    public void testExplicitResetClearsEventBuffer() {
        mEvent.setEvent("event.before.reset", "workflow", "wf-reset");
        mEvent.reset();
        // After reset mEventBuffer is empty, so start message has no event info
        String msg = mEvent.getStartEventMessage();
        assertFalse(
                msg.contains("event.before.reset"),
                "After explicit reset(), event buffer should be cleared");
    }

    // -----------------------------------------------------------------------
    // createEntityHierarchyMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateEntityHierarchyMessageContainsParentAndChild() {
        Collection<String> children = Arrays.asList("job-1", "job-2");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-1", "job", children);
        assertNotNull(msg, "createEntityHierarchyMessage should not return null");
        assertTrue(msg.contains("wf-1"), "Hierarchy message should contain parent id");
        assertTrue(msg.contains("job-1"), "Hierarchy message should contain child id");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsParentType() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-pt", "job", children);
        assertTrue(msg.contains("workflow"), "Hierarchy message should contain parent type");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildType() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-ct", "job", children);
        assertTrue(msg.contains("job"), "Hierarchy message should contain child type");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsArrow() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-arrow", "job", children);
        assertTrue(msg.contains("->"), "Hierarchy message should contain '->' separator");
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsAllChildren() {
        Collection<String> children = Arrays.asList("job-x", "job-y", "job-z");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-multi", "job", children);
        assertTrue(msg.contains("job-x"), "Hierarchy message should contain child job-x");
        assertTrue(msg.contains("job-y"), "Hierarchy message should contain child job-y");
        assertTrue(msg.contains("job-z"), "Hierarchy message should contain child job-z");
    }

    @Test
    public void testCreateEntityHierarchyMessageFormat() {
        Collection<String> children = Arrays.asList("job-1");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-fmt", "job", children);
        // Expected format: workflow<wf-fmt> -> job<job-1,>
        assertTrue(
                msg.contains("workflow<wf-fmt>"), "Hierarchy message should use <parentID> format");
        assertTrue(msg.contains("job<"), "Hierarchy message should use childType< format");
    }
}
