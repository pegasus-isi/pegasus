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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

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
        assertThat(mEvent, instanceOf(Event.class));
    }

    // -----------------------------------------------------------------------
    // Program name
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetProgramName() {
        mEvent.setProgramName("my-program");
        assertThat(mEvent.getProgramName("ignored"), is("my-program"));
    }

    // -----------------------------------------------------------------------
    // setEvent(String, String, String)
    // -----------------------------------------------------------------------

    @Test
    public void testGetStartEventMessageContainsStarted() {
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, is(notNullValue()));
        assertThat(msg, containsString("STARTED"));
    }

    @Test
    public void testStartMessageContainsEventName() {
        mEvent.setEvent("my.event.name", "wf", "wf-42");
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("my.event.name"));
    }

    @Test
    public void testStartMessageContainsEntityName() {
        mEvent.setEvent("event.test", "workflow", "wf-ent");
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("workflow"));
    }

    @Test
    public void testStartMessageContainsEntityID() {
        mEvent.setEvent("event.test", "workflow", "wf-id-999");
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("wf-id-999"));
    }

    @Test
    public void testSetEventOverwritesPreviousEvent() {
        mEvent.setEvent("event.first", "workflow", "wf-first");
        mEvent.setEvent("event.second", "workflow", "wf-second");
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("event.second"));
        assertThat(msg, not(containsString("event.first")));
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
        assertThat(msg, containsString("event.from.map"));
    }

    @Test
    public void testSetEventWithMapContainsEntityKey() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-001");
        mEvent.setEvent("event.map.test", entities);
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("workflow"));
    }

    @Test
    public void testSetEventWithMapContainsEntityValue() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-map-002");
        mEvent.setEvent("event.map.test", entities);
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("wf-map-002"));
    }

    @Test
    public void testSetEventWithMapMultipleEntries() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("workflow", "wf-multi");
        entities.put("job", "job-multi");
        mEvent.setEvent("event.multi", entities);
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("wf-multi"));
        assertThat(msg, containsString("job-multi"));
    }

    // -----------------------------------------------------------------------
    // getEndEventMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEndEventMessageContainsFinished() {
        mEvent.getStartEventMessage(); // set mStart
        String msg = mEvent.getEndEventMessage();
        assertThat(msg, is(notNullValue()));
        assertThat(msg, containsString("FINISHED"));
    }

    @Test
    public void testGetEndEventMessageContainsSeconds() {
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertThat(msg, containsString("seconds"));
    }

    @Test
    public void testGetEndEventMessageContainsEventName() {
        mEvent.setEvent("event.duration", "workflow", "wf-dur");
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertThat(msg, containsString("event.duration"));
    }

    // -----------------------------------------------------------------------
    // getEventName()
    // -----------------------------------------------------------------------

    @Test
    public void testGetEventNameReturnsNull() {
        assertThat(mEvent.getEventName(), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // add() and createLogMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testAddKeyValueAppearsInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, containsString("job-99"));
    }

    @Test
    public void testAddKeyAppearsInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, containsString("job.id"));
    }

    @Test
    public void testAddReturnsEventForChaining() {
        Event result = mEvent.add("k", "v");
        assertThat(result, is(sameInstance(mEvent)));
    }

    @Test
    public void testChainedAddAccumulatesAllValues() {
        mEvent.add("k1", "val-one").add("k2", "val-two").add("k3", "val-three");
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, containsString("val-one"));
        assertThat(logMsg, containsString("val-two"));
        assertThat(logMsg, containsString("val-three"));
    }

    @Test
    public void testCreateLogMessageIsNotNull() {
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, is(notNullValue()));
    }

    // -----------------------------------------------------------------------
    // createLogMessageAndReset()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateLogMessageAndResetReturnsAccumulatedContent() {
        mEvent.add("k", "v1");
        String msg = mEvent.createLogMessageAndReset();
        assertThat(msg, containsString("v1"));
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mEvent.add("k", "v1");
        String first = mEvent.createLogMessageAndReset();
        assertThat(first, containsString("v1"));
        String second = mEvent.createLogMessage();
        assertThat(second, not(containsString("v1")));
    }

    // -----------------------------------------------------------------------
    // reset()
    // -----------------------------------------------------------------------

    @Test
    public void testExplicitResetClearsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        mEvent.reset();
        String msg = mEvent.createLogMessage();
        assertThat(msg, not(containsString("stale-value")));
    }

    @Test
    public void testExplicitResetClearsEventBuffer() {
        mEvent.setEvent("event.before.reset", "workflow", "wf-reset");
        mEvent.reset();
        // After reset mEventBuffer is empty, so start message has no event info
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, not(containsString("event.before.reset")));
    }

    // -----------------------------------------------------------------------
    // createEntityHierarchyMessage()
    // -----------------------------------------------------------------------

    @Test
    public void testCreateEntityHierarchyMessageContainsParentAndChild() {
        Collection<String> children = Arrays.asList("job-1", "job-2");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-1", "job", children);
        assertThat(msg, is(notNullValue()));
        assertThat(msg, containsString("wf-1"));
        assertThat(msg, containsString("job-1"));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsParentType() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-pt", "job", children);
        assertThat(msg, containsString("workflow"));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildType() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-ct", "job", children);
        assertThat(msg, containsString("job"));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsArrow() {
        Collection<String> children = Arrays.asList("job-a");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-arrow", "job", children);
        assertThat(msg, containsString("->"));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsAllChildren() {
        Collection<String> children = Arrays.asList("job-x", "job-y", "job-z");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-multi", "job", children);
        assertThat(msg, containsString("job-x"));
        assertThat(msg, containsString("job-y"));
        assertThat(msg, containsString("job-z"));
    }

    @Test
    public void testCreateEntityHierarchyMessageFormat() {
        Collection<String> children = Arrays.asList("job-1");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-fmt", "job", children);
        // Expected format: workflow<wf-fmt> -> job<job-1,>
        assertThat(msg, containsString("workflow<wf-fmt>"));
        assertThat(msg, containsString("job<"));
    }

    @Test
    public void testDefaultConstructorLeavesProgramNameUnset() {
        SimpleEvent event = new SimpleEvent();
        assertThat(event.getProgramName("ignored"), is(nullValue()));
    }

    @Test
    public void testConstructorInitializesBuffersAndTimes() throws Exception {
        SimpleEvent event = new SimpleEvent();
        assertThat(ReflectionTestUtils.getField(event, "mEventBuffer"), is(notNullValue()));
        assertThat(ReflectionTestUtils.getField(event, "mLogBuffer"), is(notNullValue()));
        assertThat(ReflectionTestUtils.getField(event, "mStart"), is(0.0d));
        assertThat(ReflectionTestUtils.getField(event, "mEnd"), is(0.0d));
    }

    @Test
    public void testSetEventWithEmptyMapLeavesOnlyEventNameInStartMessage() {
        mEvent.setEvent("event.empty.map", new HashMap<String, String>());
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, containsString("event.empty.map"));
        assertThat(msg, containsString("STARTED"));
    }

    @Test
    public void testCreateEntityHierarchyMessageWithNoChildrenUsesEmptyChildSection() {
        String msg =
                mEvent.createEntityHierarchyMessage("workflow", "wf-empty", "job", Arrays.asList());
        assertThat(msg, is("workflow<wf-empty> -> job<>"));
    }
}
