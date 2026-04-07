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
package edu.isi.pegasus.planner.invocation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import org.griphyn.vdl.Chimera;
import org.junit.jupiter.api.Test;

/** Tests for Invocation abstract class structure. */
public class InvocationTest {

    @Test
    public void testImplementsSerializable() {
        assertThat(Serializable.class.isAssignableFrom(Invocation.class), is(true));
    }

    @Test
    public void testInvocationIsAbstract() {
        assertThat(Modifier.isAbstract(Invocation.class.getModifiers()), is(true));
    }

    @Test
    public void testExtendsChimera() {
        assertThat(Chimera.class.isAssignableFrom(Invocation.class), is(true));
    }

    @Test
    public void testArchitectureExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Architecture.class), is(true));
    }

    @Test
    public void testDataExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Data.class), is(true));
    }

    @Test
    public void testJobExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Job.class), is(true));
    }

    @Test
    public void testUsageExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Usage.class), is(true));
    }

    @Test
    public void testStatInfoExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(StatInfo.class), is(true));
    }

    @Test
    public void testEnvironmentExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Environment.class), is(true));
    }

    @Test
    public void testCommandLineExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(CommandLine.class), is(true));
    }
}
