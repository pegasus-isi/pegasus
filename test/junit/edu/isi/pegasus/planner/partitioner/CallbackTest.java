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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CallbackTest {

    @Test
    public void testCallbackIsInterface() {
        assertThat(Callback.class.isInterface(), is(true));
    }

    @Test
    public void testMethodSignatures() throws Exception {
        Method cbPartition = Callback.class.getDeclaredMethod("cbPartition", Partition.class);
        Method cbParents = Callback.class.getDeclaredMethod("cbParents", String.class, List.class);
        Method cbDone = Callback.class.getDeclaredMethod("cbDone");

        assertThat(cbPartition.getReturnType(), is(void.class));
        assertThat(cbParents.getReturnType(), is(void.class));
        assertThat(cbDone.getReturnType(), is(void.class));
        assertThat(Modifier.isPublic(cbPartition.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(cbPartition.getModifiers()), is(true));
        assertThat(Modifier.isPublic(cbParents.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(cbParents.getModifiers()), is(true));
        assertThat(Modifier.isPublic(cbDone.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(cbDone.getModifiers()), is(true));
    }

    @Test
    public void testCallbackDeclaresOnlyExpectedMethods() {
        assertThat(Callback.class.getDeclaredMethods().length, is(3));
    }
}
