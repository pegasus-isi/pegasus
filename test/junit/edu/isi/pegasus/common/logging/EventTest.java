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
package edu.isi.pegasus.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.format.NetloggerEvent;
import edu.isi.pegasus.common.logging.format.SimpleEvent;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Structural tests for the Event interface via reflection. */
public class EventTest {

    // --- interface structure ---

    @Test
    public void testEventIsInterface() {
        assertTrue(Event.class.isInterface());
    }

    @Test
    public void testEventExtendsCloneable() {
        assertThat(Event.class.getInterfaces(), hasItemInArray(Cloneable.class));
    }

    // --- method declarations (NoSuchMethodException = test failure if missing) ---

    @Test
    public void testNoArgMethods() throws NoSuchMethodException {
        Event.class.getMethod("getEventName");
        Event.class.getMethod("getStartEventMessage");
        Event.class.getMethod("getEndEventMessage");
        Event.class.getMethod("reset");
        Event.class.getMethod("createLogMessage");
        Event.class.getMethod("createLogMessageAndReset");
    }

    @Test
    public void testProgramNameMethods() throws NoSuchMethodException {
        Event.class.getMethod("setProgramName", String.class);
        Event.class.getMethod("getProgramName", String.class);
    }

    @Test
    public void testAddMethod() throws NoSuchMethodException {
        Event.class.getMethod("add", String.class, String.class);
    }

    @Test
    public void testSetEventMethods() throws NoSuchMethodException {
        Event.class.getMethod("setEvent", String.class, String.class, String.class);
        Event.class.getMethod("setEvent", String.class, Map.class);
    }

    @Test
    public void testCreateEntityHierarchyMessageMethod() throws NoSuchMethodException {
        Event.class.getMethod(
                "createEntityHierarchyMessage",
                String.class,
                String.class,
                String.class,
                Collection.class);
    }

    // --- known implementors ---

    @Test
    public void testKnownImplementors() {
        assertThat(NetloggerEvent.class, typeCompatibleWith(Event.class));
        assertThat(SimpleEvent.class, typeCompatibleWith(Event.class));
    }
}
