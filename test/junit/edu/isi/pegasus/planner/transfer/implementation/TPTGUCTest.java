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
package edu.isi.pegasus.planner.transfer.implementation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TPTGUCTest {

    @Test
    public void testTPTGUCExtendsGUC() {
        assertThat(TPTGUC.class.getSuperclass(), is(GUC.class));
    }

    @Test
    public void testConstructorSignature() throws Exception {
        Constructor<TPTGUC> constructor = TPTGUC.class.getDeclaredConstructor(PegasusBag.class);

        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
        assertThat(constructor.getParameterCount(), is(1));
        assertThat(TPTGUC.class.getDeclaredConstructors().length, is(1));
    }

    @Test
    public void testSelectedMethodSignatures() throws Exception {
        Method useThirdPartyTransferAlways =
                TPTGUC.class.getDeclaredMethod("useThirdPartyTransferAlways");
        assertThat(useThirdPartyTransferAlways.getReturnType(), is(boolean.class));
        assertThat(Modifier.isPublic(useThirdPartyTransferAlways.getModifiers()), is(true));

        Method generateArgumentString =
                TPTGUC.class.getDeclaredMethod("generateArgumentString", TransferJob.class);
        assertThat(generateArgumentString.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(generateArgumentString.getModifiers()), is(true));

        Method postProcess = TPTGUC.class.getDeclaredMethod("postProcess", TransferJob.class);
        assertThat(postProcess.getReturnType(), is(void.class));
        assertThat(Modifier.isPublic(postProcess.getModifiers()), is(true));
    }

    @Test
    public void testTPTGUCDeclaresExpectedMethods() {
        Method[] methods = TPTGUC.class.getDeclaredMethods();
        assertThat(methods.length, is(3));
    }
}
