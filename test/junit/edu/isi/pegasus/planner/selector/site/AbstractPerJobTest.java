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
 * Tests for the AbstractPerJob site selector class. Tests are exercised via the concrete Random
 * subclass.
 */
public class AbstractPerJobTest {

    @Test
    public void testRandomIsAbstractPerJob() {
        Random selector = new Random();
        assertThat(selector, instanceOf(AbstractPerJob.class));
    }

    @Test
    public void testRoundRobinIsAbstractPerJob() {
        RoundRobin selector = new RoundRobin();
        assertThat(selector, instanceOf(AbstractPerJob.class));
    }

    @Test
    public void testGroupIsAbstractSiteSelector() {
        Group selector = new Group();
        assertThat(selector, instanceOf(Abstract.class));
    }

    @Test
    public void testRandomImplementsSiteSelector() {
        Random selector = new Random();
        assertThat(selector, instanceOf(SiteSelector.class));
    }

    @Test
    public void testRoundRobinDescription() {
        RoundRobin selector = new RoundRobin();
        String desc = selector.description();
        assertThat(desc, not(isEmptyOrNullString()));
    }

    @Test
    public void testRandomDescription() {
        Random selector = new Random();
        String desc = selector.description();
        assertThat(desc, not(isEmptyOrNullString()));
    }

    @Test
    public void testAbstractPerJobIsAbstractAndExtendsAbstract() {
        assertThat(Modifier.isAbstract(AbstractPerJob.class.getModifiers()), is(true));
        assertThat(Abstract.class.isAssignableFrom(AbstractPerJob.class), is(true));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                AbstractPerJob.class
                        .getMethod(
                                "mapWorkflow",
                                edu.isi.pegasus.planner.classes.ADag.class,
                                java.util.List.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(
                AbstractPerJob.class
                        .getMethod(
                                "mapJob",
                                edu.isi.pegasus.planner.classes.Job.class,
                                java.util.List.class)
                        .getReturnType(),
                is(Void.TYPE));
    }

    @Test
    public void testMapJobIsAbstract() throws Exception {
        assertThat(
                Modifier.isAbstract(
                        AbstractPerJob.class
                                .getMethod(
                                        "mapJob",
                                        edu.isi.pegasus.planner.classes.Job.class,
                                        java.util.List.class)
                                .getModifiers()),
                is(true));
    }
}
