/*
 * Copyright 2007-20120 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.classes;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.dax.Invoke;
import java.io.IOException;
import java.util.Collection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class NotificationsTest {

    public NotificationsTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testNotificationsDeserialization() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = // "hooks:\n"
                "  shell:\n"
                        + "    - _on: start\n"
                        + "      cmd: /bin/date\n"
                        + "    - _on: end\n"
                        + "      cmd: /bin/echo \"Finished\"\n";

        Notifications n = mapper.readValue(test, Notifications.class);
        assertNotNull(n);
        assertTrue(n.contains(new Invoke(Invoke.WHEN.start, "/bin/date")));
        assertTrue(n.contains(new Invoke(Invoke.WHEN.end, "/bin/echo \"Finished\"")));
    }

    @Test
    public void testNotificationsSerialization() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.add(new Invoke(Invoke.WHEN.end, "/bin/echo \"Finished\""));

        String expected =
                "---\n"
                        + "shell:\n"
                        + " -\n"
                        + "  _on: \"start\"\n"
                        + "  cmd: \"/bin/date\"\n"
                        + "  _on: \"end\"\n"
                        + "  cmd: \"/bin/echo \\\"Finished\\\"\"\n"
                        + "";
        String actual = mapper.writeValueAsString(n);
        System.out.println(actual);
        assertEquals(expected, actual);
    }

    // ---- Additional tests ----

    @Test
    public void testDefaultConstructorIsEmpty() {
        Notifications n = new Notifications();
        assertTrue(n.isEmpty());
    }

    @Test
    public void testAddSingleNotification() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertFalse(n.isEmpty());
    }

    @Test
    public void testAddNullNotificationIsNoop() {
        Notifications n = new Notifications();
        n.add(null); // should not throw
        assertTrue(n.isEmpty());
    }

    @Test
    public void testContainsReturnsTrueForAddedInvoke() {
        Notifications n = new Notifications();
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/date");
        n.add(invoke);
        assertTrue(n.contains(invoke));
    }

    @Test
    public void testContainsReturnsFalseForAbsentInvoke() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertFalse(n.contains(new Invoke(Invoke.WHEN.end, "/bin/date")));
    }

    @Test
    public void testGetNotificationsByWhenEnum() {
        Notifications n = new Notifications();
        Invoke iv1 = new Invoke(Invoke.WHEN.start, "/bin/date");
        Invoke iv2 = new Invoke(Invoke.WHEN.end, "/bin/echo done");
        n.add(iv1);
        n.add(iv2);

        Collection<Invoke> startNotifications = n.getNotifications(Invoke.WHEN.start);
        assertNotNull(startNotifications);
        assertTrue(startNotifications.contains(iv1));
        assertFalse(startNotifications.contains(iv2));
    }

    @Test
    public void testGetNotificationsByWhenString() {
        Notifications n = new Notifications();
        Invoke iv = new Invoke(Invoke.WHEN.success, "/bin/notify-success");
        n.add(iv);

        Collection<Invoke> successNotifications = n.getNotifications("success");
        assertNotNull(successNotifications);
        assertTrue(successNotifications.contains(iv));
    }

    @Test
    public void testGetNotificationsEmptyCollectionForUnusedWhen() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Collection<Invoke> endNotifications = n.getNotifications(Invoke.WHEN.end);
        assertNotNull(endNotifications);
        assertTrue(endNotifications.isEmpty());
    }

    @Test
    public void testAddAllFromAnotherNotifications() {
        Notifications source = new Notifications();
        source.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        source.add(new Invoke(Invoke.WHEN.end, "/bin/echo done"));

        Notifications target = new Notifications();
        target.addAll(source);

        assertFalse(target.isEmpty());
        assertTrue(target.contains(new Invoke(Invoke.WHEN.start, "/bin/date")));
        assertTrue(target.contains(new Invoke(Invoke.WHEN.end, "/bin/echo done")));
    }

    @Test
    public void testAddAllNullIsNoop() {
        Notifications n = new Notifications();
        n.addAll(null); // should not throw
        assertTrue(n.isEmpty());
    }

    @Test
    public void testResetClearsAllNotifications() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertFalse(n.isEmpty());
        n.reset();
        assertTrue(n.isEmpty());
    }

    @Test
    public void testClonePreservesNotifications() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.add(new Invoke(Invoke.WHEN.end, "/bin/echo done"));

        Notifications clone = (Notifications) n.clone();
        assertNotNull(clone);
        assertTrue(clone.contains(new Invoke(Invoke.WHEN.start, "/bin/date")));
        assertTrue(clone.contains(new Invoke(Invoke.WHEN.end, "/bin/echo done")));
    }

    @Test
    public void testCloneHasSameContents() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Notifications clone = (Notifications) n.clone();

        // The clone should contain the same notifications as the original
        assertTrue(clone.contains(new Invoke(Invoke.WHEN.start, "/bin/date")));
        assertFalse(clone.isEmpty());
    }

    @Test
    public void testToStringContainsAddedInvokes() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        String s = n.toString();
        assertNotNull(s);
        assertTrue(s.contains("/bin/date"));
        assertTrue(s.contains("start"));
    }

    @Test
    public void testIsEmptyAfterAddAndReset() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.reset();
        assertTrue(n.isEmpty());
    }

    @Test
    public void testAliasedWhenAtEndMapsToEnd() {
        // Invoke.setWhen maps at_end -> end
        Invoke iv = new Invoke(Invoke.WHEN.at_end, "/bin/cleanup");
        assertEquals("end", iv.getWhen());

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> endNotifs = n.getNotifications(Invoke.WHEN.end);
        assertFalse(endNotifs.isEmpty());
    }

    @Test
    public void testAliasedWhenOnSuccessMapsToSuccess() {
        Invoke iv = new Invoke(Invoke.WHEN.on_success, "/bin/notify");
        assertEquals("success", iv.getWhen());

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> successNotifs = n.getNotifications(Invoke.WHEN.success);
        assertFalse(successNotifs.isEmpty());
    }

    @Test
    public void testAliasedWhenOnErrorMapsToError() {
        Invoke iv = new Invoke(Invoke.WHEN.on_error, "/bin/handle-error");
        assertEquals("error", iv.getWhen());

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> errorNotifs = n.getNotifications(Invoke.WHEN.error);
        assertFalse(errorNotifs.isEmpty());
    }

    @Test
    public void testMultipleNotificationsForSameWhen() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.add(new Invoke(Invoke.WHEN.start, "/bin/hostname"));

        Collection<Invoke> startNotifs = n.getNotifications(Invoke.WHEN.start);
        assertEquals(2, startNotifs.size());
    }
}
