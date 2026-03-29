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

/** Tests for the SiteSelector interface constants. */
public class SiteSelectorTest {

    @Test
    public void testVersionConstant() {
        assertNotNull(SiteSelector.VERSION, "VERSION constant should not be null");
        assertFalse(SiteSelector.VERSION.isEmpty(), "VERSION constant should not be empty");
    }

    @Test
    public void testVersionFormat() {
        assertTrue(
                SiteSelector.VERSION.matches("\\d+\\.\\d+"),
                "VERSION should be in numeric dotted format like '2.0'");
    }

    @Test
    public void testSiteNotFoundConstantNotNull() {
        assertNotNull(SiteSelector.SITE_NOT_FOUND, "SITE_NOT_FOUND constant should not be null");
    }

    @Test
    public void testSiteNotFoundValue() {
        assertEquals("NONE", SiteSelector.SITE_NOT_FOUND, "SITE_NOT_FOUND should be 'NONE'");
    }

    @Test
    public void testSiteNotFoundNotEmpty() {
        assertFalse(SiteSelector.SITE_NOT_FOUND.isEmpty(), "SITE_NOT_FOUND should not be empty");
    }
}
