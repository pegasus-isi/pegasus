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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ClassADSGenerator class constants and structure. */
public class ClassADSGeneratorTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testGeneratorConstant() {
        assertEquals("Pegasus", ClassADSGenerator.GENERATOR);
    }

    @Test
    public void testGeneratorAdKeyConstant() {
        assertEquals("pegasus_generator", ClassADSGenerator.GENERATOR_AD_KEY);
    }

    @Test
    public void testVersionAdKeyConstant() {
        assertEquals("pegasus_version", ClassADSGenerator.VERSION_AD_KEY);
    }

    @Test
    public void testRootWfUuidKeyConstant() {
        assertEquals("pegasus_root_wf_uuid", ClassADSGenerator.ROOT_WF_UUID_KEY);
    }

    @Test
    public void testWfUuidKeyConstant() {
        assertEquals("pegasus_wf_uuid", ClassADSGenerator.WF_UUID_KEY);
    }

    @Test
    public void testWfNameAdKeyConstant() {
        assertEquals("pegasus_wf_name", ClassADSGenerator.WF_NAME_AD_KEY);
    }

    @Test
    public void testWfTimeAdKeyConstant() {
        assertEquals("pegasus_wf_time", ClassADSGenerator.WF_TIME_AD_KEY);
    }

    @Test
    public void testXformationAdKeyConstant() {
        assertEquals("pegasus_wf_xformation", ClassADSGenerator.XFORMATION_AD_KEY);
    }
}
