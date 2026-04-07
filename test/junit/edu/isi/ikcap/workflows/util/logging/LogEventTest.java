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
package edu.isi.ikcap.workflows.util.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class LogEventTest {

    @Test
    public void testSingleEntityConstructorInitializesFields() throws Exception {
        LogEvent event = new LogEvent("event.sample", "planner", "job.id", "ID0001");

        assertThat(getField(event, "_eventName"), is("event.sample"));
        assertThat(getField(event, "_progName"), is("planner"));
        assertThat((String) getField(event, "_eventId"), startsWith("event.sample_"));

        Map<?, ?> entityMap = (Map<?, ?>) getField(event, "_entityIdMap");
        assertThat(entityMap.size(), is(1));
        assertThat(entityMap.get("job.id"), is("ID0001"));
    }

    @Test
    public void testCreateStartLogMessageIncludesProgramAndEntityIds() {
        LogEvent event = new LogEvent("event.sample", "planner", "job.id", "ID0001");

        String message = event.createStartLogMsg().toString();

        assertThat(message, containsString("event=event.sample.start"));
        assertThat(message, containsString("prog=planner"));
        assertThat(message, containsString("job.id=ID0001"));
        assertThat(message, containsString("ts="));
    }

    @Test
    public void testCreateLogAndEndMessagesUseCurrentEventNames() {
        LogEvent event = new LogEvent("event.sample", "planner", "job.id", "ID0001");

        String logMessage = event.createLogMsg().toString();
        String endMessage = event.createEndLogMsg().toString();

        assertThat(logMessage, containsString("event=event.sample "));
        assertThat(logMessage, not(containsString("prog=planner")));
        assertThat(endMessage, containsString("event=event.sample.end"));
        assertThat(endMessage, containsString("job.id=ID0001"));
    }

    @Test
    public void testMapConstructorUsesProvidedEntityMapContents() {
        Map<String, String> entities = new HashMap<String, String>();
        entities.put("dax.id", "dax-42");
        entities.put("request.id", "request-7");

        LogEvent event = new LogEvent("event.aggregate", "planner", entities);
        String message = event.createLogMsg().toString();

        assertThat(message, containsString("dax.id=dax-42"));
        assertThat(message, containsString("request.id=request-7"));
    }

    @Test
    public void testCreateIdHierarchyLogMessageUsesCurrentChildFormatting() {
        String message =
                LogEvent.createIdHierarchyLogMsg(
                                "parent.type",
                                "parent-1",
                                "child.type",
                                Arrays.asList("childA", "childB").iterator())
                        .toString();

        assertThat(message, containsString("event=event.id.creation"));
        assertThat(message, containsString("parent.id.type=parent.type"));
        assertThat(message, containsString("parent.id=parent-1"));
        assertThat(message, containsString("child.ids.type=child.type"));
        assertThat(message, containsString("child.ids={childA,childB,}"));
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }
}
