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
import static org.hamcrest.Matchers.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for HasDescriptor interface structure. */
public class HasDescriptorTest {

    @Test
    public void testHasDescriptorIsInterface() {
        assertThat(HasDescriptor.class.isInterface(), is(true));
    }

    @Test
    public void testHasGetDescriptorMethod() throws Exception {
        Method m = HasDescriptor.class.getMethod("getDescriptor");
        assertThat(m, notNullValue());
        assertThat(m.getReturnType(), is(int.class));
    }

    @Test
    public void testHasSetDescriptorMethod() throws Exception {
        Method m = HasDescriptor.class.getMethod("setDescriptor", int.class);
        assertThat(m, notNullValue());
        assertThat(m.getReturnType(), is(void.class));
    }

    @Test
    public void testDescriptorImplementsInterface() {
        assertThat(HasDescriptor.class.isAssignableFrom(Descriptor.class), is(true));
    }

    @Test
    public void testFifoImplementsInterface() {
        assertThat(HasDescriptor.class.isAssignableFrom(Fifo.class), is(true));
    }

    @Test
    public void testDescriptorGetDescriptorDefault() {
        Descriptor d = new Descriptor();
        assertThat(d.getDescriptor(), is(-1));
    }

    @Test
    public void testDescriptorSetDescriptor() {
        Descriptor d = new Descriptor();
        d.setDescriptor(7);
        assertThat(d.getDescriptor(), is(7));
    }

    @Test
    public void testInterfaceMethodsArePublicAndAbstract() throws Exception {
        Method getter = HasDescriptor.class.getMethod("getDescriptor");
        Method setter = HasDescriptor.class.getMethod("setDescriptor", int.class);

        assertThat(Modifier.isPublic(getter.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(getter.getModifiers()), is(true));
        assertThat(Modifier.isPublic(setter.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(setter.getModifiers()), is(true));
    }

    @Test
    public void testFifoSetDescriptorUpdatesDescriptorValue() {
        Fifo fifo = new Fifo();
        fifo.setDescriptor(11);

        assertThat(fifo.getDescriptor(), is(11));
    }
}
