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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Braindump code generator class constants and structure. */
public class BraindumpTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testBraindumpFileConstant() {
        assertEquals("braindump.yml", Braindump.BRAINDUMP_FILE);
    }

    @Test
    public void testGeneratorTypeKeyConstant() {
        assertEquals("type", Braindump.GENERATOR_TYPE_KEY);
    }

    @Test
    public void testUserKeyConstant() {
        assertEquals("user", Braindump.USER_KEY);
    }

    @Test
    public void testUUIDKeyConstant() {
        assertEquals("wf_uuid", Braindump.UUID_KEY);
    }

    @Test
    public void testRootUUIDKeyConstant() {
        assertEquals("root_wf_uuid", Braindump.ROOT_UUID_KEY);
    }

    @Test
    public void testDAXLabelKeyConstant() {
        assertEquals("dax_label", Braindump.DAX_LABEL_KEY);
    }

    @Test
    public void testSubmitDirKeyConstant() {
        assertEquals("submit_dir", Braindump.SUBMIT_DIR_KEY);
    }

    @Test
    public void testPlannerVersionKeyConstant() {
        assertEquals("planner_version", Braindump.PLANNER_VERSION_KEY);
    }

    @Test
    public void testTimestampKeyConstant() {
        assertEquals("timestamp", Braindump.TIMESTAMP_KEY);
    }

    @Test
    public void testPropertiesKeyConstant() {
        assertEquals("properties", Braindump.PROPERTIES_KEY);
    }
}
