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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class T2Test {

    @Test
    public void testT2ExtendsAbstractMultipleFTPerXFERJob() {
        assertThat(T2.class.getSuperclass(), is(AbstractMultipleFTPerXFERJob.class));
    }

    @Test
    public void testConstantsAndDeclaredFields() throws Exception {
        assertThat(T2.TRANSFORMATION_NAMESPACE, is("pegasus"));
        assertThat(T2.TRANSFORMATION_NAME, is("T2"));
        assertThat(T2.TRANSFORMATION_VERSION, nullValue());
        assertThat(T2.DERIVATION_NAMESPACE, is("pegasus"));
        assertThat(T2.DERIVATION_NAME, is("T2"));
        assertThat(T2.DERIVATION_VERSION, is("1.0"));
        assertThat(T2.DESCRIPTION, is("Pegasus T2"));

        Field numProcessesField = T2.class.getDeclaredField("mNumOfTXProcesses");
        assertThat(numProcessesField.getType(), is(String.class));

        Field numStreamsField = T2.class.getDeclaredField("mNumOfTXStreams");
        assertThat(numStreamsField.getType(), is(String.class));

        Field useForceField = T2.class.getDeclaredField("mUseForce");
        assertThat(useForceField.getType(), is(boolean.class));
    }

    @Test
    public void testConstructorAndSelectedMethodSignatures() throws Exception {
        Constructor<T2> constructor = T2.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
        assertThat(constructor.getParameterCount(), is(1));

        Method useThirdPartyTransferAlways =
                T2.class.getDeclaredMethod("useThirdPartyTransferAlways");
        assertThat(useThirdPartyTransferAlways.getReturnType(), is(boolean.class));
        assertThat(Modifier.isPublic(useThirdPartyTransferAlways.getModifiers()), is(true));

        Method doesPreserveXBit = T2.class.getDeclaredMethod("doesPreserveXBit");
        assertThat(doesPreserveXBit.getReturnType(), is(boolean.class));

        Method getDescription = T2.class.getDeclaredMethod("getDescription");
        assertThat(getDescription.getReturnType(), is(String.class));

        Method getTransformationCatalogEntry =
                T2.class.getDeclaredMethod(
                        "getTransformationCatalogEntry", String.class, int.class);
        assertThat(
                getTransformationCatalogEntry.getReturnType(),
                is(
                        edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry
                                .class));

        Method generateArgumentString =
                T2.class.getDeclaredMethod("generateArgumentString", TransferJob.class);
        assertThat(generateArgumentString.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(generateArgumentString.getModifiers()), is(true));
    }

    @Test
    public void testProtectedHelperMethodSignatures() throws Exception {
        Method writeStdInAndAssociateCredentials =
                T2.class.getDeclaredMethod(
                        "writeStdInAndAssociateCredentials",
                        TransferJob.class,
                        java.io.FileWriter.class,
                        java.util.Collection.class,
                        String.class,
                        int.class);
        assertThat(writeStdInAndAssociateCredentials.getReturnType(), is(void.class));
        assertThat(
                Modifier.isProtected(writeStdInAndAssociateCredentials.getModifiers()), is(true));

        Method getCompleteTCName = T2.class.getDeclaredMethod("getCompleteTCName");
        assertThat(getCompleteTCName.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(getCompleteTCName.getModifiers()), is(true));

        Method getEnvironmentVariables =
                T2.class.getDeclaredMethod("getEnvironmentVariables", String.class);
        assertThat(getEnvironmentVariables.getReturnType(), is(java.util.List.class));
        assertThat(Modifier.isProtected(getEnvironmentVariables.getModifiers()), is(true));

        Method getDerivationNamespace = T2.class.getDeclaredMethod("getDerivationNamespace");
        assertThat(getDerivationNamespace.getReturnType(), is(String.class));

        Method getDerivationName = T2.class.getDeclaredMethod("getDerivationName");
        assertThat(getDerivationName.getReturnType(), is(String.class));

        Method getDerivationVersion = T2.class.getDeclaredMethod("getDerivationVersion");
        assertThat(getDerivationVersion.getReturnType(), is(String.class));
    }
}
