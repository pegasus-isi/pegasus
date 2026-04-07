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
package edu.isi.pegasus.planner.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Structural tests for MainEngine. */
public class MainEngineTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(MainEngine.class), is(true));
    }

    @Test
    public void testCleanupDirConstant() {
        assertThat(MainEngine.CLEANUP_DIR, is("cleanup"));
    }

    @Test
    public void testCatalogsDirBasenameConstant() {
        assertThat(MainEngine.CATALOGS_DIR_BASENAME, is("catalogs"));
    }

    @Test
    public void testHasAdagAndPegasusBagConstructor() throws Exception {
        Constructor<MainEngine> constructor =
                MainEngine.class.getDeclaredConstructor(ADag.class, PegasusBag.class);
        assertThat(constructor, notNullValue());
    }

    @Test
    public void testRunPlannerReturnsADag() throws Exception {
        Method method = MainEngine.class.getMethod("runPlanner");
        assertThat((Object) method.getReturnType(), is((Object) ADag.class));
    }

    @Test
    public void testGetterMethodsReturnExpectedTypes() throws Exception {
        assertThat(
                (Object) MainEngine.class.getMethod("getCleanupDAG").getReturnType(),
                is((Object) ADag.class));
        assertThat(
                (Object) MainEngine.class.getMethod("getPegasusBag").getReturnType(),
                is((Object) PegasusBag.class));
    }

    @Test
    public void testSetToStringMethodSignature() throws Exception {
        Method method =
                MainEngine.class.getMethod("setToString", java.util.Set.class, String.class);
        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }
}
