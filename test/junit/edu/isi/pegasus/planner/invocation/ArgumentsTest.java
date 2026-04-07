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
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for the Arguments abstract class structure. */
public class ArgumentsTest {

    private static final class TestArguments extends Arguments {
        private String mValue;

        TestArguments() {
            super();
        }

        TestArguments(String executable) {
            super(executable);
        }

        @Override
        public String getValue() {
            return mValue;
        }

        @Override
        public void toXML(Writer stream, String indent, String namespace) {}
    }

    @Test
    public void testArgumentsIsAbstract() {
        assertThat(Modifier.isAbstract(Arguments.class.getModifiers()), is(true));
    }

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Arguments.class), is(true));
    }

    @Test
    public void testArgStringIsConcreteSubtype() {
        assertThat(Arguments.class.isAssignableFrom(ArgString.class), is(true));
        assertThat(Modifier.isAbstract(ArgString.class.getModifiers()), is(false));
    }

    @Test
    public void testArgVectorIsConcreteSubtype() {
        assertThat(Arguments.class.isAssignableFrom(ArgVector.class), is(true));
        assertThat(Modifier.isAbstract(ArgVector.class.getModifiers()), is(false));
    }

    @Test
    public void testArgStringSetExecutable() {
        ArgString as = new ArgString();
        as.setExecutable("/bin/bash");
        assertThat(as.getExecutable(), is("/bin/bash"));
    }

    @Test
    public void testArgVectorGetValueWithEntries() {
        ArgVector av = new ArgVector("/bin/test");
        av.setValue(0, "arg0");
        av.setValue(1, "arg1");
        String value = av.getValue();
        assertThat(value, containsString("arg0"));
        assertThat(value, containsString("arg1"));
    }

    @Test
    public void testArgVectorDefaultConstructor() {
        ArgVector av = new ArgVector();
        assertThat(av.getExecutable(), nullValue());
        assertThat(av.getValue(), is(""));
    }

    @Test
    public void testGetValueIsAbstractlyDeclared() throws Exception {
        java.lang.reflect.Method m = Arguments.class.getDeclaredMethod("getValue");
        assertThat(Modifier.isAbstract(m.getModifiers()), is(true));
    }

    @Test
    public void testExecutableConstructorStoresExecutable() {
        TestArguments arguments = new TestArguments("/bin/sh");

        assertThat(arguments.getExecutable(), is("/bin/sh"));
    }

    @Test
    public void testSetExecutableAllowsResetToNull() {
        TestArguments arguments = new TestArguments("/bin/sh");
        arguments.setExecutable(null);

        assertThat(arguments.getExecutable(), nullValue());
    }

    @Test
    public void testBaseToStringWriterThrowsIOException() {
        TestArguments arguments = new TestArguments();
        StringWriter writer = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> arguments.toString(writer));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }
}
