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
package edu.isi.pegasus.planner.code.generator;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.code.CodeGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Stampede code generator class. */
public class StampedeTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testImplementsCodeGenerator() {
        assertTrue(CodeGenerator.class.isAssignableFrom(Stampede.class));
    }

    @Test
    public void testNetloggerBPFileSuffixConstant() {
        assertEquals(".static.bp", Stampede.NETLOGGER_BP_FILE_SUFFIX);
    }

    @Test
    public void testNetloggerLogFormatterImplementorConstant() {
        assertEquals("Netlogger", Stampede.NETLOGGER_LOG_FORMATTER_IMPLEMENTOR);
    }

    @Test
    public void testWorkflowIdKeyConstant() {
        assertEquals("xwf.id", Stampede.WORKFLOW_ID_KEY);
    }

    @Test
    public void testTaskEventNameConstant() {
        assertEquals("task.info", Stampede.TASK_EVENT_NAME);
    }

    @Test
    public void testTaskIdKeyConstant() {
        assertEquals("task.id", Stampede.TASK_ID_KEY);
    }

    @Test
    public void testTypeKeyConstant() {
        assertEquals("type", Stampede.TYPE_KEY);
    }

    @Test
    public void testTaskTransformationKeyConstant() {
        assertEquals("transformation", Stampede.TASK_TRANSFORMATION_KEY);
    }
}
