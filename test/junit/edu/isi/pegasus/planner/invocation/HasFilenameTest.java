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

/** Tests for HasFilename interface structure. */
public class HasFilenameTest {

    @Test
    public void testHasFilenameIsInterface() {
        assertThat(HasFilename.class.isInterface(), is(true));
    }

    @Test
    public void testHasGetFilenameMethod() throws Exception {
        Method m = HasFilename.class.getMethod("getFilename");
        assertThat(m, notNullValue());
        assertThat(m.getReturnType(), is(String.class));
    }

    @Test
    public void testHasSetFilenameMethod() throws Exception {
        Method m = HasFilename.class.getMethod("setFilename", String.class);
        assertThat(m, notNullValue());
        assertThat(m.getReturnType(), is(void.class));
    }

    @Test
    public void testTemporaryImplementsInterface() {
        assertThat(HasFilename.class.isAssignableFrom(Temporary.class), is(true));
    }

    @Test
    public void testFifoImplementsInterface() {
        assertThat(HasFilename.class.isAssignableFrom(Fifo.class), is(true));
    }

    @Test
    public void testTemporarySetAndGetFilename() {
        Temporary t = new Temporary("/tmp/test.tmp", 1);
        assertThat(t.getFilename(), is("/tmp/test.tmp"));
    }

    @Test
    public void testFifoSetAndGetFilename() {
        Fifo f = new Fifo("/tmp/mypipe", 3);
        assertThat(f.getFilename(), is("/tmp/mypipe"));
    }

    @Test
    public void testInterfaceMethodsArePublicAndAbstract() throws Exception {
        Method getter = HasFilename.class.getMethod("getFilename");
        Method setter = HasFilename.class.getMethod("setFilename", String.class);

        assertThat(Modifier.isPublic(getter.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(getter.getModifiers()), is(true));
        assertThat(Modifier.isPublic(setter.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(setter.getModifiers()), is(true));
    }

    @Test
    public void testFifoSetFilenameUpdatesStoredFilename() {
        Fifo f = new Fifo("/tmp/mypipe", 3);
        f.setFilename("/tmp/otherpipe");

        assertThat(f.getFilename(), is("/tmp/otherpipe"));
    }
}
