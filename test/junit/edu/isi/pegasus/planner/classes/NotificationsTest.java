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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.dax.Invoke;
import java.io.IOException;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class NotificationsTest {

    public NotificationsTest() {}

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
        assertThat(n, is(notNullValue()));
        assertThat(n.contains(new Invoke(Invoke.WHEN.start, "/bin/date")), is(true));
        assertThat(n.contains(new Invoke(Invoke.WHEN.end, "/bin/echo \"Finished\"")), is(true));
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
        assertThat(actual, is(expected));
    }

    // ---- Additional tests ----

    @Test
    public void testDefaultConstructorIsEmpty() {
        Notifications n = new Notifications();
        assertThat(n.isEmpty(), is(true));
    }

    @Test
    public void testAddSingleNotification() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertThat(n.isEmpty(), is(false));
    }

    @Test
    public void testAddNullNotificationIsNoop() {
        Notifications n = new Notifications();
        n.add(null); // should not throw
        assertThat(n.isEmpty(), is(true));
    }

    @Test
    public void testContainsReturnsTrueForAddedInvoke() {
        Notifications n = new Notifications();
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/date");
        n.add(invoke);
        assertThat(n.contains(invoke), is(true));
    }

    @Test
    public void testContainsReturnsFalseForAbsentInvoke() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertThat(n.contains(new Invoke(Invoke.WHEN.end, "/bin/date")), is(false));
    }

    @Test
    public void testContainsReturnsFalseWhenEmpty() {
        Notifications n = new Notifications();
        assertThat(n.contains(new Invoke(Invoke.WHEN.start, "/bin/date")), is(false));
    }

    @Test
    public void testGetNotificationsByWhenEnum() {
        Notifications n = new Notifications();
        Invoke iv1 = new Invoke(Invoke.WHEN.start, "/bin/date");
        Invoke iv2 = new Invoke(Invoke.WHEN.end, "/bin/echo done");
        n.add(iv1);
        n.add(iv2);

        Collection<Invoke> startNotifications = n.getNotifications(Invoke.WHEN.start);
        assertThat(startNotifications, is(notNullValue()));
        assertThat(startNotifications.contains(iv1), is(true));
        assertThat(startNotifications.contains(iv2), is(false));
    }

    @Test
    public void testGetNotificationsByWhenString() {
        Notifications n = new Notifications();
        Invoke iv = new Invoke(Invoke.WHEN.success, "/bin/notify-success");
        n.add(iv);

        Collection<Invoke> successNotifications = n.getNotifications("success");
        assertThat(successNotifications, is(notNullValue()));
        assertThat(successNotifications.contains(iv), is(true));
    }

    @Test
    public void testGetNotificationsEmptyCollectionForUnusedWhen() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Collection<Invoke> endNotifications = n.getNotifications(Invoke.WHEN.end);
        assertThat(endNotifications, is(notNullValue()));
        assertThat(endNotifications.isEmpty(), is(true));
    }

    @Test
    public void testAddAllFromAnotherNotifications() {
        Notifications source = new Notifications();
        source.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        source.add(new Invoke(Invoke.WHEN.end, "/bin/echo done"));

        Notifications target = new Notifications();
        target.addAll(source);

        assertThat(target.isEmpty(), is(false));
        assertThat(target.contains(new Invoke(Invoke.WHEN.start, "/bin/date")), is(true));
        assertThat(target.contains(new Invoke(Invoke.WHEN.end, "/bin/echo done")), is(true));
    }

    @Test
    public void testAddAllAccumulatesExistingNotifications() {
        Notifications source = new Notifications();
        source.add(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Notifications target = new Notifications();
        target.add(new Invoke(Invoke.WHEN.start, "/bin/hostname"));
        target.addAll(source);

        assertThat(target.getNotifications(Invoke.WHEN.start).size(), is(2));
    }

    @Test
    public void testAddAllNullIsNoop() {
        Notifications n = new Notifications();
        n.addAll(null); // should not throw
        assertThat(n.isEmpty(), is(true));
    }

    @Test
    public void testResetClearsAllNotifications() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        assertThat(n.isEmpty(), is(false));
        n.reset();
        assertThat(n.isEmpty(), is(true));
    }

    @Test
    public void testResetLeavesCollectionsAvailableForEveryWhen() {
        Notifications n = new Notifications();
        n.reset();

        for (Invoke.WHEN when : Invoke.WHEN.values()) {
            assertThat(n.getNotifications(when), is(notNullValue()));
        }
    }

    @Test
    public void testClonePreservesNotifications() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.add(new Invoke(Invoke.WHEN.end, "/bin/echo done"));

        Notifications clone = (Notifications) n.clone();
        assertThat(clone, is(notNullValue()));
        assertThat(clone.contains(new Invoke(Invoke.WHEN.start, "/bin/date")), is(true));
        assertThat(clone.contains(new Invoke(Invoke.WHEN.end, "/bin/echo done")), is(true));
    }

    @Test
    public void testCloneHasSameContents() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Notifications clone = (Notifications) n.clone();

        // The clone should contain the same notifications as the original
        assertThat(clone.contains(new Invoke(Invoke.WHEN.start, "/bin/date")), is(true));
        assertThat(clone.isEmpty(), is(false));
    }

    @Test
    public void testToStringContainsAddedInvokes() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        String s = n.toString();
        assertThat(s, is(notNullValue()));
        assertThat(s.contains("/bin/date"), is(true));
        assertThat(s.contains("start"), is(true));
    }

    @Test
    public void testToStringEmptyNotificationsIsEmptyString() {
        Notifications n = new Notifications();
        assertThat(n.toString(), is(""));
    }

    @Test
    public void testIsEmptyAfterAddAndReset() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.reset();
        assertThat(n.isEmpty(), is(true));
    }

    @Test
    public void testAliasedWhenAtEndMapsToEnd() {
        // Invoke.setWhen maps at_end -> end
        Invoke iv = new Invoke(Invoke.WHEN.at_end, "/bin/cleanup");
        assertThat(iv.getWhen(), is("end"));

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> endNotifs = n.getNotifications(Invoke.WHEN.end);
        assertThat(endNotifs.isEmpty(), is(false));
    }

    @Test
    public void testAliasedWhenOnSuccessMapsToSuccess() {
        Invoke iv = new Invoke(Invoke.WHEN.on_success, "/bin/notify");
        assertThat(iv.getWhen(), is("success"));

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> successNotifs = n.getNotifications(Invoke.WHEN.success);
        assertThat(successNotifs.isEmpty(), is(false));
    }

    @Test
    public void testAliasedWhenOnErrorMapsToError() {
        Invoke iv = new Invoke(Invoke.WHEN.on_error, "/bin/handle-error");
        assertThat(iv.getWhen(), is("error"));

        Notifications n = new Notifications();
        n.add(iv);
        Collection<Invoke> errorNotifs = n.getNotifications(Invoke.WHEN.error);
        assertThat(errorNotifs.isEmpty(), is(false));
    }

    @Test
    public void testMultipleNotificationsForSameWhen() {
        Notifications n = new Notifications();
        n.add(new Invoke(Invoke.WHEN.start, "/bin/date"));
        n.add(new Invoke(Invoke.WHEN.start, "/bin/hostname"));

        Collection<Invoke> startNotifs = n.getNotifications(Invoke.WHEN.start);
        assertThat(startNotifs.size(), is(2));
    }
}
