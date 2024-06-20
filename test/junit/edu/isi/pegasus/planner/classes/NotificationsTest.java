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
}
