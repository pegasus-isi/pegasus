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

import edu.isi.ikcap.workflows.util.logging.EventLogMessage;
import edu.isi.ikcap.workflows.util.logging.LogEvent;
import edu.isi.pegasus.common.logging.Event;
import java.lang.reflect.Field;
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
        assertThat(Modifier.isAbstract(NetloggerEvent.class.getModifiers()), is(false));
    }

    @Test
    public void testNetloggerEventImplementsEvent() {
        assertThat(mEvent, instanceOf(Event.class));
    }

    @Test
    public void testSetAndGetProgramName() {
        mEvent.setProgramName("my-prog");
        assertThat(mEvent.getProgramName("ignored"), is("my-prog"));
    }

    @Test
    public void testGetStartEventMessageIsNotNull() {
        String msg = mEvent.getStartEventMessage();
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testGetEndEventMessageIsNotNull() {
        mEvent.getStartEventMessage();
        String msg = mEvent.getEndEventMessage();
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testAddKeyValueInLogMessage() {
        mEvent.add("job.id", "job-99");
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, is(notNullValue()));
        assertThat(logMsg, containsString("job-99"));
    }

    @Test
    public void testCreateLogMessageAndResetClearsBuffer() {
        mEvent.add("key", "value1");
        String firstMsg = mEvent.createLogMessageAndReset();
        assertThat(firstMsg, containsString("value1"));
        // After reset, value1 should not be in next message
        String secondMsg = mEvent.createLogMessage();
        assertThat(secondMsg, not(containsString("value1")));
    }

    @Test
    public void testAddReturnsEventForChaining() {
        Event result = mEvent.add("key", "val");
        assertThat(result, is(sameInstance(mEvent)));
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
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testSetEventWithMapCreatesValidEndMessage() {
        NetloggerEvent event = new NetloggerEvent();
        event.setProgramName("test-pegasus");
        Map<String, String> entities = new HashMap<>();
        entities.put("workflow", "wf-map-002");
        event.setEvent("event.pegasus.map", entities);
        String msg = event.getEndEventMessage();
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testSetEventWithMapResetsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        Map<String, String> entities = new HashMap<>();
        entities.put("workflow", "wf-reset-001");
        mEvent.setEvent("event.pegasus.reset", entities);
        String msg = mEvent.createLogMessage();
        assertThat(msg, not(containsString("stale-value")));
    }

    @Test
    public void testSetEventWithStringResetsLogBuffer() {
        mEvent.add("stale.key", "stale-value");
        mEvent.setEvent("event.pegasus.plan2", "workflow", "wf-002");
        String msg = mEvent.createLogMessage();
        assertThat(msg, not(containsString("stale-value")));
    }

    @Test
    public void testGetEventNameReturnsNull() {
        assertThat(mEvent.getEventName(), is(nullValue()));
    }

    @Test
    public void testChainedAddProducesAllValues() {
        mEvent.add("k1", "v1").add("k2", "v2").add("k3", "v3");
        String logMsg = mEvent.createLogMessage();
        assertThat(logMsg, containsString("v1"));
        assertThat(logMsg, containsString("v2"));
        assertThat(logMsg, containsString("v3"));
    }

    @Test
    public void testCreateEntityHierarchyMessageIsNotNull() {
        List<String> children = Arrays.asList("job-1", "job-2", "job-3");
        String msg = mEvent.createEntityHierarchyMessage("workflow", "wf-001", "job", children);
        assertThat(msg, is(notNullValue()));
        assertThat(msg.isEmpty(), is(false));
    }

    @Test
    public void testCreateEntityHierarchyMessageContainsChildIDs() {
        List<String> children = Arrays.asList("job-alpha", "job-beta");
        String msg =
                mEvent.createEntityHierarchyMessage("workflow", "wf-hier-001", "job", children);
        assertThat(msg, anyOf(containsString("job-alpha"), containsString("job-beta")));
    }

    @Test
    public void testResetProducesCleanLogMessage() {
        mEvent.add("before.reset", "dirty-value");
        mEvent.reset();
        String msg = mEvent.createLogMessage();
        assertThat(msg, not(containsString("dirty-value")));
    }

    @Test
    public void testDefaultConstructorLeavesProgramNameUnset() {
        NetloggerEvent event = new NetloggerEvent();
        assertThat(event.getProgramName("ignored"), is(nullValue()));
    }

    @Test
    public void testDeclaresExpectedPrivateFields() throws Exception {
        Field programField = NetloggerEvent.class.getDeclaredField("mProgram");
        Field logEventField = NetloggerEvent.class.getDeclaredField("mLogEvent");
        Field messageField = NetloggerEvent.class.getDeclaredField("mMessage");

        assertThat(programField.getType(), is(String.class));
        assertThat(logEventField.getType(), is(LogEvent.class));
        assertThat(messageField.getType(), is(EventLogMessage.class));
        assertThat(Modifier.isPrivate(programField.getModifiers()), is(true));
        assertThat(Modifier.isPrivate(logEventField.getModifiers()), is(true));
        assertThat(Modifier.isPrivate(messageField.getModifiers()), is(true));
    }

    @Test
    public void testCreateEntityHierarchyMessageWithEmptyChildrenStillIncludesParent() {
        String msg =
                mEvent.createEntityHierarchyMessage(
                        "workflow", "wf-empty-001", "job", Arrays.asList());
        assertThat(msg, is(notNullValue()));
        assertThat(msg, containsString("wf-empty-001"));
    }
}
