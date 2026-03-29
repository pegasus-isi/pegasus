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
package edu.isi.pegasus.planner.selector.site;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.selector.SiteSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Group site selector. */
public class GroupTest {

    private Group mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Group();
    }

    @Test
    public void testDescription() {
        String desc = mSelector.description();
        assertNotNull(desc, "Description should not be null");
        assertFalse(desc.isEmpty(), "Description should not be empty");
    }

    @Test
    public void testDescriptionContainsGroup() {
        String desc = mSelector.description().toLowerCase();
        assertTrue(desc.contains("group"), "Description should mention 'group'");
    }

    @Test
    public void testImplementsSiteSelector() {
        assertInstanceOf(SiteSelector.class, mSelector, "Group should implement SiteSelector");
    }

    @Test
    public void testExtendsAbstract() {
        assertInstanceOf(Abstract.class, mSelector, "Group should extend Abstract");
    }

    @Test
    public void testInstantiationWithDefaultConstructor() {
        Group selector = new Group();
        assertNotNull(selector, "Group should be instantiatable with no-arg constructor");
    }

    @Test
    public void testDescriptionIsNotNull() {
        assertNotNull(mSelector.description());
    }
}
