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
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Abstract site selector class. Uses concrete subclasses to exercise the abstract
 * base.
 */
public class AbstractTest {

    @Test
    public void testRandomImplementsAbstract() {
        Random selector = new Random();
        assertThat(selector, instanceOf(Abstract.class));
    }

    @Test
    public void testRoundRobinImplementsAbstract() {
        RoundRobin selector = new RoundRobin();
        assertThat(selector, instanceOf(Abstract.class));
    }

    @Test
    public void testGroupImplementsAbstract() {
        Group selector = new Group();
        assertThat(selector, instanceOf(Abstract.class));
    }

    @Test
    public void testAbstractImplementsSiteSelector() {
        Random selector = new Random();
        assertThat(selector, instanceOf(SiteSelector.class));
    }

    @Test
    public void testRandomCanBeInstantiated() {
        assertDoesNotThrow(Random::new, "Random should be instantiatable with no-arg constructor");
    }

    @Test
    public void testRoundRobinCanBeInstantiated() {
        assertDoesNotThrow(
                RoundRobin::new, "RoundRobin should be instantiatable with no-arg constructor");
    }

    @Test
    public void testGroupCanBeInstantiated() {
        assertDoesNotThrow(Group::new, "Group should be instantiatable with no-arg constructor");
    }

    @Test
    public void testAbstractIsAbstract() {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
    }

    @Test
    public void testInitializeMethodReturnsVoid() throws Exception {
        assertThat(
                Abstract.class
                        .getMethod("initialize", edu.isi.pegasus.planner.classes.PegasusBag.class)
                        .getReturnType(),
                is(Void.TYPE));
    }

    @Test
    public void testProtectedFieldsExistWithExpectedTypes() throws Exception {
        assertThat(
                Abstract.class.getDeclaredField("mProps").getType(),
                is(edu.isi.pegasus.planner.common.PegasusProperties.class));
        assertThat(
                Abstract.class.getDeclaredField("mLogger").getType(),
                is(edu.isi.pegasus.common.logging.LogManager.class));
        assertThat(
                Abstract.class.getDeclaredField("mSiteStore").getType(),
                is(edu.isi.pegasus.planner.catalog.site.classes.SiteStore.class));
        assertThat(
                Abstract.class.getDeclaredField("mTCMapper").getType(),
                is(edu.isi.pegasus.planner.catalog.transformation.Mapper.class));
        assertThat(
                Abstract.class.getDeclaredField("mBag").getType(),
                is(edu.isi.pegasus.planner.classes.PegasusBag.class));

        assertThat(
                Modifier.isProtected(Abstract.class.getDeclaredField("mProps").getModifiers()),
                is(true));
        assertThat(
                Modifier.isProtected(Abstract.class.getDeclaredField("mLogger").getModifiers()),
                is(true));
    }
}
