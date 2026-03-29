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
package edu.isi.pegasus.planner.selector;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelector interface constants. */
public class ReplicaSelectorTest {

    @Test
    public void testVersionConstant() {
        assertNotNull(ReplicaSelector.VERSION, "VERSION constant should not be null");
        assertFalse(ReplicaSelector.VERSION.isEmpty(), "VERSION constant should not be empty");
    }

    @Test
    public void testVersionFormat() {
        // Version should match major.minor format
        assertTrue(
                ReplicaSelector.VERSION.matches("\\d+\\.\\d+"),
                "VERSION should be in numeric dotted format like '1.6'");
    }

    @Test
    public void testLocalSiteHandleValue() {
        assertEquals(
                "local", ReplicaSelector.LOCAL_SITE_HANDLE, "LOCAL_SITE_HANDLE should be 'local'");
    }

    @Test
    public void testLocalSiteHandleNotEmpty() {
        assertFalse(
                ReplicaSelector.LOCAL_SITE_HANDLE.isEmpty(),
                "LOCAL_SITE_HANDLE should not be empty");
    }

    @Test
    public void testPriorityKeyNotNull() {
        assertNotNull(ReplicaSelector.PRIORITY_KEY, "PRIORITY_KEY constant should not be null");
    }

    @Test
    public void testPriorityKeyValue() {
        assertEquals("priority", ReplicaSelector.PRIORITY_KEY, "PRIORITY_KEY should be 'priority'");
    }
}
