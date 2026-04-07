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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        assertThat(mSelector, notNullValue());
    }

    @Test
    public void testDescription() {
        String desc = mSelector.description();
        assertThat(desc, not(isEmptyOrNullString()));
    }

    @Test
    public void testDescriptionContainsRandom() {
        String desc = mSelector.description().toLowerCase();
        assertThat(desc.toLowerCase(), containsString("random"));
    }

    @Test
    public void testImplementsSiteSelector() {
        assertThat(mSelector, instanceOf(SiteSelector.class));
    }

    @Test
    public void testExtendsAbstractPerJob() {
        assertThat(mSelector, instanceOf(AbstractPerJob.class));
    }

    @Test
    public void testExtendsAbstract() {
        assertThat(mSelector, instanceOf(Abstract.class));
    }

    @Test
    public void testDescriptionExactValue() {
        assertThat(mSelector.description(), is("Random Site Selection"));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                Random.class
                        .getMethod("initialize", edu.isi.pegasus.planner.classes.PegasusBag.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(
                Random.class
                        .getMethod(
                                "mapJob",
                                edu.isi.pegasus.planner.classes.Job.class,
                                java.util.List.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(Random.class.getMethod("description").getReturnType(), is(String.class));
    }

    @Test
    public void testPrivateSelectRandomSiteMethodExists() throws Exception {
        assertThat(
                Random.class
                        .getDeclaredMethod("selectRandomSite", java.util.List.class)
                        .getReturnType(),
                is(String.class));
    }
}
