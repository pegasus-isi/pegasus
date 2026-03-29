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

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for Invocation abstract class structure. */
public class InvocationTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(Invocation.class.getModifiers()));
    }

    @Test
    public void testImplementsSerializable() {
        assertTrue(Serializable.class.isAssignableFrom(Invocation.class));
    }

    @Test
    public void testArchitectureExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Architecture.class));
    }

    @Test
    public void testDataExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Data.class));
    }

    @Test
    public void testJobExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Job.class));
    }

    @Test
    public void testUsageExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Usage.class));
    }

    @Test
    public void testStatInfoExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(StatInfo.class));
    }

    @Test
    public void testEnvironmentExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Environment.class));
    }
}
