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
package edu.isi.pegasus.planner.refiner.createdir;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for AbstractStrategy. */
public class AbstractStrategyTest {

    @Test
    public void testImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(AbstractStrategy.class), is(true));
    }

    @Test
    public void testCreateDirSuffixConstant() {
        assertThat(AbstractStrategy.CREATE_DIR_SUFFIX, is("_cdir"));
    }

    @Test
    public void testCreateDirPrefixConstant() {
        assertThat(AbstractStrategy.CREATE_DIR_PREFIX, is("create_dir_"));
    }

    @Test
    public void testHourGlassExtendsAbstractStrategy() {
        assertThat(AbstractStrategy.class.isAssignableFrom(HourGlass.class), is(true));
    }

    @Test
    public void testMinimalExtendsAbstractStrategy() {
        assertThat(AbstractStrategy.class.isAssignableFrom(Minimal.class), is(true));
    }

    @Test
    public void testAbstractStrategyIsAbstract() {
        assertThat(Modifier.isAbstract(AbstractStrategy.class.getModifiers()), is(true));
    }

    @Test
    public void testInitializeAndHelperMethodSignatures() throws Exception {
        assertThat(
                (Object)
                        AbstractStrategy.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                                        Implementation.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        AbstractStrategy.class
                                .getMethod(
                                        "getCreateDirJobName",
                                        edu.isi.pegasus.planner.classes.ADag.class,
                                        String.class)
                                .getReturnType(),
                is((Object) String.class));
        assertThat(
                (Object)
                        AbstractStrategy.class
                                .getMethod(
                                        "getCreateDirSites",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) java.util.Set.class));
    }

    @Test
    public void testGetCreateDirSitesIsStatic() throws Exception {
        assertThat(
                Modifier.isStatic(
                        AbstractStrategy.class
                                .getMethod(
                                        "getCreateDirSites",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getModifiers()),
                is(true));
    }
}
