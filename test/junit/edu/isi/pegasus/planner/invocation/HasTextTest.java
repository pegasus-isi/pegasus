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
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for HasText interface structure. */
public class HasTextTest {

    @Test
    public void testHasTextIsInterface() {
        assertThat(HasText.class.isInterface(), is(true));
    }

    @Test
    public void testHasAppendValueMethod() throws Exception {
        Method m = HasText.class.getMethod("appendValue", String.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testHasGetValueMethod() throws Exception {
        Method m = HasText.class.getMethod("getValue");
        assertThat(m, is(notNullValue()));
        assertThat(m.getReturnType(), is(String.class));
    }

    @Test
    public void testHasSetValueMethod() throws Exception {
        Method m = HasText.class.getMethod("setValue", String.class);
        assertThat(m, is(notNullValue()));
        assertThat(m.getReturnType(), is(void.class));
    }

    @Test
    public void testDataImplementsInterface() {
        assertThat(HasText.class.isAssignableFrom(Data.class), is(true));
    }

    @Test
    public void testArgEntryImplementsInterface() {
        assertThat(HasText.class.isAssignableFrom(ArgEntry.class), is(true));
    }

    @Test
    public void testArchitectureImplementsInterface() {
        assertThat(HasText.class.isAssignableFrom(Architecture.class), is(true));
    }

    @Test
    public void testDataAppendAndGetValue() {
        Data d = new Data();
        d.appendValue("hello");
        d.appendValue(" world");
        assertThat(d.getValue(), is("hello world"));
    }

    @Test
    public void testInterfaceMethodsArePublicAndAbstract() throws Exception {
        Method append = HasText.class.getMethod("appendValue", String.class);
        Method get = HasText.class.getMethod("getValue");
        Method set = HasText.class.getMethod("setValue", String.class);

        assertThat(Modifier.isPublic(append.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(append.getModifiers()), is(true));
        assertThat(Modifier.isPublic(get.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(get.getModifiers()), is(true));
        assertThat(Modifier.isPublic(set.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(set.getModifiers()), is(true));
    }

    @Test
    public void testBootSetValueAndGetValueThroughInterfaceContract() {
        Boot boot = new Boot();
        boot.setValue("2024-01-01");

        assertThat(boot.getValue(), is("2024-01-01"));
    }
}
