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
import org.junit.jupiter.api.Test;

/** Tests for the Heft site selector class. */
public class HeftTest {

    @Test
    public void testHeftExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Heft.class), is(true));
    }

    @Test
    public void testHeftImplementsSiteSelector() {
        assertThat(SiteSelector.class.isAssignableFrom(Heft.class), is(true));
    }

    @Test
    public void testHeftInstantiation() {
        Heft heft = new Heft();
        assertThat(heft, notNullValue());
    }

    @Test
    public void testDescriptionExactValue() {
        assertThat(new Heft().description(), is("Heft based Site Selector"));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                Heft.class
                        .getMethod("initialize", edu.isi.pegasus.planner.classes.PegasusBag.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(
                Heft.class
                        .getMethod(
                                "mapWorkflow",
                                edu.isi.pegasus.planner.classes.ADag.class,
                                java.util.List.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(
                Heft.class
                        .getMethod(
                                "mapWorkflow",
                                edu.isi.pegasus.planner.classes.ADag.class,
                                java.util.List.class,
                                String.class)
                        .getReturnType(),
                is(Void.TYPE));
        assertThat(Heft.class.getMethod("description").getReturnType(), is(String.class));
    }

    @Test
    public void testPrivateHeftImplementationFieldExists() throws Exception {
        assertThat(
                Heft.class.getDeclaredField("mHeftImpl").getType(),
                is(edu.isi.pegasus.planner.selector.site.heft.Algorithm.class));
    }
}
