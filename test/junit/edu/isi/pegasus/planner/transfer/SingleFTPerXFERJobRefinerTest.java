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
package edu.isi.pegasus.planner.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.implementation.TransferImplementationFactoryException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SingleFTPerXFERJobRefinerTest {

    @Test
    public void testSingleFTPerXFERJobRefinerIsAbstractAndExtendsAbstractRefiner() {
        assertThat(Modifier.isAbstract(SingleFTPerXFERJobRefiner.class.getModifiers()), is(true));
        assertThat(
                SingleFTPerXFERJobRefiner.class.getSuperclass(),
                sameInstance(AbstractRefiner.class));
    }

    @Test
    public void testConstructorSignature() throws Exception {
        Constructor<SingleFTPerXFERJobRefiner> constructor =
                SingleFTPerXFERJobRefiner.class.getDeclaredConstructor(
                        ADag.class, PegasusBag.class);

        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
        assertThat(constructor.getParameterCount(), equalTo(2));
        assertThat(SingleFTPerXFERJobRefiner.class.getDeclaredConstructors().length, equalTo(1));
    }

    @Test
    public void testLoadImplementationsMethodSignature() throws Exception {
        Method method =
                SingleFTPerXFERJobRefiner.class.getDeclaredMethod(
                        "loadImplementations", PegasusBag.class);

        assertThat(method.getReturnType(), equalTo(void.class));
        assertThat(Modifier.isPublic(method.getModifiers()), is(true));
        assertThat(method.getExceptionTypes().length, equalTo(1));
        assertThat(
                method.getExceptionTypes()[0],
                sameInstance(TransferImplementationFactoryException.class));
    }
}
