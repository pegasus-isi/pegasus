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
package edu.isi.pegasus.planner.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for PegasusDBAdmin static constants and structural properties.
 *
 * @author Rajiv Mayani
 */
public class PegasusDBAdminTest {

    @Test
    public void testMasterDatabasePropertyKey() {
        assertEquals(
                "pegasus.catalog.master.url",
                PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY,
                "Master database property key should match");
    }

    @Test
    public void testMasterDatabaseDeprecatedPropertyKey() {
        assertEquals(
                "pegasus.dashboard.output",
                PegasusDBAdmin.MASTER_DATABASE_DEPRECATED_PROPERTY_KEY,
                "Deprecated master database property key should match");
    }

    @Test
    public void testWorkflowDatabasePropertyKey() {
        assertEquals(
                "pegasus.catalog.workflow.url",
                PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY,
                "Workflow database property key should match");
    }

    @Test
    public void testWorkflowDatabaseDeprecatedPropertyKey() {
        assertEquals(
                "pegasus.monitord.output",
                PegasusDBAdmin.WORKFLOW_DATABASE_DEPRECATED_PROPERTY_KEY,
                "Deprecated workflow database property key should match");
    }

    @Test
    public void testMasterDatabaseKeyIsNotEmpty() {
        assertFalse(
                PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY.isEmpty(),
                "Master database property key should not be empty");
    }

    @Test
    public void testWorkflowDatabaseKeyIsNotEmpty() {
        assertFalse(
                PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY.isEmpty(),
                "Workflow database property key should not be empty");
    }

    @Test
    public void testPropertyKeysAreDistinct() {
        assertNotEquals(
                PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY,
                PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY,
                "Master and workflow property keys should be different");
    }

    @Test
    public void testPegasusDBAdminIsConcreteClass() {
        assertFalse(
                java.lang.reflect.Modifier.isAbstract(PegasusDBAdmin.class.getModifiers()),
                "PegasusDBAdmin should be a concrete (non-abstract) class");
    }
}
