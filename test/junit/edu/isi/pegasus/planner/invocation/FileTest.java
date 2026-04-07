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

/** Tests for File abstract invocation class structure. */
public class FileTest {

    private static final class TestFile extends File {
        TestFile() {
            super();
        }

        TestFile(String value) {
            super(value);
        }

        @Override
        public void toXML(Writer stream, String indent, String namespace) {}
    }

    @Test
    public void testFileIsAbstract() {
        assertThat(Modifier.isAbstract(File.class.getModifiers()), is(true));
    }

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(File.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(File.class), is(true));
    }

    @Test
    public void testDescriptorIsConcreteSubclass() {
        assertThat(File.class.isAssignableFrom(Descriptor.class), is(true));
        assertThat(Modifier.isAbstract(Descriptor.class.getModifiers()), is(false));
    }

    @Test
    public void testTemporaryExtendsFile() {
        assertThat(File.class.isAssignableFrom(Temporary.class), is(true));
    }

    @Test
    public void testRegularExtendsFile() {
        assertThat(File.class.isAssignableFrom(Regular.class), is(true));
    }

    @Test
    public void testDescriptorAppendValueBuildsHexbyte() {
        Descriptor d = new Descriptor();
        d.appendValue("deadbeef");
        assertThat(d.getValue(), is("deadbeef"));
    }

    @Test
    public void testDescriptorAppendNullIsNoop() {
        Descriptor d = new Descriptor();
        d.appendValue(null);
        assertThat(d.getValue(), nullValue());
    }

    @Test
    public void testBaseConstructorWithValueStoresHexBytes() {
        TestFile file = new TestFile("deadbeef");

        assertThat(file.getValue(), is("deadbeef"));
    }

    @Test
    public void testSetValueReplacesExistingHexBytes() {
        TestFile file = new TestFile("dead");
        file.setValue("beef");

        assertThat(file.getValue(), is("beef"));
    }

    @Test
    public void testBaseToStringWriterThrowsIOException() {
        TestFile file = new TestFile("deadbeef");
        StringWriter writer = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> file.toString(writer));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }
}
