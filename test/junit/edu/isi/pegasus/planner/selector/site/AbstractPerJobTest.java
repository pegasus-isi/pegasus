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
import org.junit.jupiter.api.Test;

/**
 * Tests for the AbstractPerJob site selector class. Tests are exercised via the concrete Random
 * subclass.
 */
public class AbstractPerJobTest {

    @Test
    public void testRandomIsAbstractPerJob() {
        Random selector = new Random();
        assertInstanceOf(
                AbstractPerJob.class, selector, "Random should be a subclass of AbstractPerJob");
    }

    @Test
    public void testRoundRobinIsAbstractPerJob() {
        RoundRobin selector = new RoundRobin();
        assertInstanceOf(
                AbstractPerJob.class,
                selector,
                "RoundRobin should be a subclass of AbstractPerJob");
    }

    @Test
    public void testGroupIsAbstractSiteSelector() {
        Group selector = new Group();
        assertInstanceOf(
                Abstract.class, selector, "Group should be a subclass of Abstract site selector");
    }

    @Test
    public void testRandomImplementsSiteSelector() {
        Random selector = new Random();
        assertInstanceOf(SiteSelector.class, selector, "Random should implement SiteSelector");
    }

    @Test
    public void testRoundRobinDescription() {
        RoundRobin selector = new RoundRobin();
        String desc = selector.description();
        assertNotNull(desc, "Description should not be null");
        assertFalse(desc.isEmpty(), "Description should not be empty");
    }

    @Test
    public void testRandomDescription() {
        Random selector = new Random();
        String desc = selector.description();
        assertNotNull(desc, "Description should not be null");
        assertFalse(desc.isEmpty(), "Description should not be empty");
    }
}
