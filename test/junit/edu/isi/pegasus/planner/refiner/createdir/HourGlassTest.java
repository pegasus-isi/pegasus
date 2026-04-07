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

import org.junit.jupiter.api.Test;

/** Structural tests for HourGlass createdir strategy. */
public class HourGlassTest {

    @Test
    public void testExtendsAbstractStrategy() {
        assertThat(AbstractStrategy.class.isAssignableFrom(HourGlass.class), is(true));
    }

    @Test
    public void testImplementsStrategy() {
        assertThat(Strategy.class.isAssignableFrom(HourGlass.class), is(true));
    }

    @Test
    public void testDummyConcatJobConstant() {
        assertThat(HourGlass.DUMMY_CONCAT_JOB, is("pegasus_concat"));
    }

    @Test
    public void testDummyConcatJobPrefixConstant() {
        assertThat(HourGlass.DUMMY_CONCAT_JOB_PREFIX, is("pegasus_concat_"));
    }

    @Test
    public void testTransformationNamespace() {
        assertThat(HourGlass.TRANSFORMATION_NAMESPACE, is("pegasus"));
    }

    @Test
    public void testDefaultConstructor() {
        HourGlass hg = new HourGlass();
        assertThat(hg, notNullValue());
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(HourGlass.TRANSFORMATION_NAME, is("dirmanager"));
        assertThat(HourGlass.TRANSFORMATION_VERSION, nullValue());
        assertThat(HourGlass.COMPLETE_TRANSFORMATION_NAME, is("pegasus::dirmanager"));
        assertThat(HourGlass.DERIVATION_NAMESPACE, is("pegasus"));
        assertThat(HourGlass.DERIVATION_NAME, is("dirmanager"));
        assertThat(HourGlass.DERIVATION_VERSION, is("1.0"));
    }

    @Test
    public void testPublicMethodReturnTypes() throws Exception {
        assertThat(
                (Object)
                        HourGlass.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                                        Implementation.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        HourGlass.class
                                .getMethod(
                                        "addCreateDirectoryNodes",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.ADag.class));
        assertThat(
                (Object)
                        HourGlass.class
                                .getMethod(
                                        "makeDummyConcatJob",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testHelperMethodsExist() throws Exception {
        assertThat(
                (Object)
                        HourGlass.class
                                .getDeclaredMethod(
                                        "getConcatJobname",
                                        edu.isi.pegasus.planner.classes.ADag.class)
                                .getReturnType(),
                is((Object) String.class));
        assertThat(
                (Object)
                        HourGlass.class
                                .getDeclaredMethod(
                                        "introduceRootDependencies",
                                        edu.isi.pegasus.planner.classes.ADag.class,
                                        edu.isi.pegasus.planner.classes.Job.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
        assertThat(
                (Object)
                        HourGlass.class
                                .getDeclaredMethod(
                                        "construct",
                                        edu.isi.pegasus.planner.classes.Job.class,
                                        String.class,
                                        String.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
    }
}
