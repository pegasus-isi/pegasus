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

/** Tests for the Random site selector. */
public class RandomTest {

    private Random mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Random();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mSelector, "Random site selector should be instantiatable");
    }

    @Test
    public void testDescription() {
        String desc = mSelector.description();
        assertNotNull(desc, "Description should not be null");
        assertFalse(desc.isEmpty(), "Description should not be empty");
    }

    @Test
    public void testDescriptionContainsRandom() {
        String desc = mSelector.description().toLowerCase();
        assertTrue(desc.contains("random"), "Description should mention 'random'");
    }

    @Test
    public void testImplementsSiteSelector() {
        assertInstanceOf(SiteSelector.class, mSelector, "Random should implement SiteSelector");
    }

    @Test
    public void testExtendsAbstractPerJob() {
        assertInstanceOf(AbstractPerJob.class, mSelector, "Random should extend AbstractPerJob");
    }

    @Test
    public void testExtendsAbstract() {
        assertInstanceOf(Abstract.class, mSelector, "Random should extend Abstract");
    }
}
