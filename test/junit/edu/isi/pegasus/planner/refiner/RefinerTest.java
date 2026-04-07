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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for Refiner interface. */
public class RefinerTest {

    @Test
    public void testVersionConstant() {
        assertThat(Refiner.VERSION, is("1.0"));
    }

    @Test
    public void testHasGetWorkflowMethod() throws Exception {
        assertThat(Refiner.class.getMethod("getWorkflow"), notNullValue());
    }

    @Test
    public void testGetWorkflowReturnsADag() throws Exception {
        assertThat(
                (Object) Refiner.class.getMethod("getWorkflow").getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.ADag.class));
    }

    @Test
    public void testRefinerIsInterface() {
        assertThat(Refiner.class.isInterface(), is(true));
    }

    @Test
    public void testGetWorkflowMethodIsPublicAndAbstract() throws Exception {
        Method method = Refiner.class.getMethod("getWorkflow");
        assertThat(Modifier.isPublic(method.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(method.getModifiers()), is(true));
    }

    @Test
    public void testRefinerDeclaresOnlyGetWorkflowMethod() {
        assertThat(Refiner.class.getDeclaredMethods().length, is(1));
    }
}
