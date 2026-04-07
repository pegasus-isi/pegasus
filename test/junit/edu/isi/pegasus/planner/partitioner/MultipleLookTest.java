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
package edu.isi.pegasus.planner.partitioner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.griphyn.vdl.euryale.Callback;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class MultipleLookTest {

    @Test
    public void testMultipleLookExtendsDAXWriter() {
        assertThat(DAXWriter.class.isAssignableFrom(MultipleLook.class), is(true));
    }

    @Test
    public void testConstructorInitializesFieldsAndDefaultIndex() throws Exception {
        MultipleLook writer = new MultipleLook("workflow.dax", "/tmp/pdax");
        assertThat(ReflectionTestUtils.getField(writer, "mDaxFile"), is("workflow.dax"));
        assertThat(ReflectionTestUtils.getField(writer, "mPDAXDirectory"), is("/tmp/pdax"));
        assertThat(ReflectionTestUtils.getField(writer, "mIndex"), is(-1));
    }

    @Test
    public void testWritePartitionDaxMethodSignature() throws Exception {
        Method method =
                MultipleLook.class.getDeclaredMethod(
                        "writePartitionDax", Partition.class, int.class);

        assertThat(method.getReturnType(), is(boolean.class));
        assertThat(method.getParameterCount(), is(2));
    }

    @Test
    public void testPrivateCallbackHandlerExistsAndImplementsCallback() throws Exception {
        Class<?> callbackHandler = null;
        for (Class<?> inner : MultipleLook.class.getDeclaredClasses()) {
            if (inner.getSimpleName().equals("MyCallBackHandler")) {
                callbackHandler = inner;
                break;
            }
        }

        assertThat(callbackHandler, is(notNullValue()));
        assertThat(Callback.class.isAssignableFrom(callbackHandler), is(true));

        Constructor<?> constructor = callbackHandler.getDeclaredConstructors()[0];
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        assertThat(parameterTypes.length, is(1));
        assertThat(parameterTypes[0], is(MultipleLook.class));
    }
}
