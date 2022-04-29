/**
 * Copyright 2007-2021 University Of Southern California
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
package edu.isi.pegasus.planner.dax;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class JobTest {

    public JobTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
    public void tearDown() {}

    @Test
    public void testJobArgumentsSerialization() {
        Job j = new Job("ID001", "test");
        j.addArgument("--force");
        j.addArgument("-q");
        j.addArgument("--cleanup none");

        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        j.toXML(writer);
        String result = sw.toString();

        // System.out.println(result);
        String expected =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<job id=\"ID001\" name=\"test\">\n"
                        + "   <argument>--force -q --cleanup none</argument>\n"
                        + "</job>\n";

        // chop the xml comments
        result = chopComments(result);
        expected = chopComments(expected);

        System.out.println(result);

        assertEquals(expected, result);
    }

    @Test
    public void testJobArgumentsWithFileSerialization() {
        Job j = new Job("ID001", "test");
        j.addArgument("-i");
        j.addArgument(new File("f.a"));
        j.addArgument("--cleanup none");

        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        j.toXML(writer);
        String result = sw.toString();

        // System.out.println(result);
        String expected =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<job id=\"ID001\" name=\"test\">\n"
                        + "   <argument>-i <file name=\"f.a\"/> --cleanup none</argument>\n"
                        + "</job>\n";

        // chop the xml comments
        result = chopComments(result);
        expected = chopComments(expected);

        System.out.println(result);

        assertEquals(expected, result);
    }

    @Test
    public void testJobArgumentsWithMultipleFilesSerialization() {
        Job j = new Job("ID001", "test");
        j.addArgument("-i");
        List<File> inputs = new LinkedList();
        inputs.add(new File("f.a"));
        inputs.add(new File("f.b"));
        j.addArgument(inputs);
        j.addArgument("--cleanup none");

        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        j.toXML(writer);
        String result = sw.toString();

        // System.out.println(result);
        String expected =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<job id=\"ID001\" name=\"test\">\n"
                        + "   <argument>-i <file name=\"f.a\"/> <file name=\"f.b\"/> --cleanup none</argument>\n"
                        + "</job>\n";

        // chop the xml comments
        result = chopComments(result);
        expected = chopComments(expected);

        System.out.println(result);

        assertEquals(expected, result);
    }

    protected String chopComments(String input) {
        String pattern1 = "<!--.*-->\n";
        input = input.replaceAll(pattern1, "");
        return input;
    }
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
