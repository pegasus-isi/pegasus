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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ExampleDAXCallbackTest {

    @Test
    public void testInitializeIsNoOpAndDoneStartsFalse() throws Exception {
        ExampleDAXCallback callback = new ExampleDAXCallback();

        callback.initialize(new PegasusBag(), "workflow.dax");

        assertThat(getDoneField(callback), is(false));
    }

    @Test
    public void testCbDoneSetsDoneFlag() throws Exception {
        ExampleDAXCallback callback = new ExampleDAXCallback();

        callback.cbDone();

        assertThat(getDoneField(callback), is(true));
    }

    @Test
    public void testGetConstructedObjectReturnsCurrentPlaceholderString() {
        ExampleDAXCallback callback = new ExampleDAXCallback();

        Object constructed = callback.getConstructedObject();

        assertThat(constructed instanceof String, is(true));
        assertThat(constructed, is("Shallow Object"));
    }

    @Test
    public void testDocumentJobAndChildrenCallbacksPrintExpectedMarkers() {
        ExampleDAXCallback callback = new ExampleDAXCallback();
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("name", "wf");

        Job job = new Job();
        job.setLogicalID("ID0001");
        job.logicalName = "preprocess";

        PrintStream originalOut = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        try {
            System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8.name()));
            callback.cbDocument(attributes);
            callback.cbJob(job);
            callback.cbChildren("parent", Arrays.asList("childA", "childB"));
        } catch (Exception e) {
            fail("printing callbacks should not throw: " + e.getMessage());
        } finally {
            System.setOut(originalOut);
        }

        String output = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        assertThat(output.contains("The attributes in DAX header retrieved"), is(true));
        assertThat(output.contains("{name=wf}"), is(true));
        assertThat(output.contains("Job parsed"), is(true));
        assertThat(output.contains("Edges in the DAX"), is(true));
        assertThat(output.contains("Parent -> parent"), is(true));
        assertThat(output.contains("\t -> childA"), is(true));
        assertThat(output.contains("\t -> childB"), is(true));
    }

    private boolean getDoneField(ExampleDAXCallback callback) throws Exception {
        return ((Boolean) ReflectionTestUtils.getField(callback, "mDone")).booleanValue();
    }
}
