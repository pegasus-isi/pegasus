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

/** Structural tests for RemoveDirectory refiner. */
public class RemoveDirectoryTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(RemoveDirectory.class), is(true));
    }

    @Test
    public void testHasAddRemoveDirectoryNodesMethod() throws Exception {
        assertThat(
                RemoveDirectory.class.getMethod(
                        "addRemoveDirectoryNodes", edu.isi.pegasus.planner.classes.ADag.class),
                notNullValue());
    }

    @Test
    public void testConstants() {
        assertThat(RemoveDirectory.TRANSFORMATION_NAME, is("cleanup"));
        assertThat(RemoveDirectory.REMOVE_DIR_EXECUTABLE_BASENAME, is("pegasus-transfer"));
        assertThat(RemoveDirectory.CLEANUP_PREFIX, is("cleanup_"));
        assertThat(RemoveDirectory.DERIVATION_VERSION, is("1.0"));
    }

    @Test
    public void testHasConstructorWithDagBagAndSubmitDirectory() throws Exception {
        Constructor<RemoveDirectory> constructor =
                RemoveDirectory.class.getDeclaredConstructor(
                        ADag.class, PegasusBag.class, String.class);
        assertThat(constructor, notNullValue());
    }

    @Test
    public void testOverloadedAddRemoveDirectoryNodesMethodSignature() throws Exception {
        Method method =
                RemoveDirectory.class.getMethod(
                        "addRemoveDirectoryNodes", ADag.class, java.util.Set.class);
        assertThat((Object) method.getReturnType(), is((Object) ADag.class));
    }

    @Test
    public void testGetCompleteTransformationNameReturnsString() throws Exception {
        Method method = RemoveDirectory.class.getMethod("getCompleteTranformationName");
        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }
}
