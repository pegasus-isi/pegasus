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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class GUCTest {

    @Test
    public void testGUCExtendsAbstractMultipleFTPerXFERJob() {
        assertThat(GUC.class.getSuperclass(), is(AbstractMultipleFTPerXFERJob.class));
    }

    @Test
    public void testConstantsAndDeclaredFields() throws Exception {
        assertThat(GUC.TRANSFORMATION_NAMESPACE, is("globus"));
        assertThat(GUC.TRANSFORMATION_NAME, is("guc"));
        assertThat(GUC.TRANSFORMATION_VERSION, is(nullValue()));
        assertThat(GUC.DERIVATION_NAMESPACE, is("globus"));
        assertThat(GUC.DERIVATION_NAME, is("guc"));
        assertThat(GUC.DERIVATION_VERSION, is(nullValue()));
        assertThat(
                "GUC client that supports multiple file transfers. Available in globus 4.x series",
                is(GUC.DESCRIPTION));

        Field numStreamsField = GUC.class.getDeclaredField("mNumOfTXStreams");
        assertThat(numStreamsField.getType(), is(String.class));

        Field useForceField = GUC.class.getDeclaredField("mUseForce");
        assertThat(useForceField.getType(), is(boolean.class));
    }

    @Test
    public void testConstructorAndSelectedMethodSignatures() throws Exception {
        Constructor<GUC> constructor = GUC.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));
        assertThat(constructor.getParameterCount(), is(1));

        Method useThirdPartyTransferAlways =
                GUC.class.getDeclaredMethod("useThirdPartyTransferAlways");
        assertThat(useThirdPartyTransferAlways.getReturnType(), is(boolean.class));
        assertThat(Modifier.isPublic(useThirdPartyTransferAlways.getModifiers()), is(true));

        Method doesPreserveXBit = GUC.class.getDeclaredMethod("doesPreserveXBit");
        assertThat(doesPreserveXBit.getReturnType(), is(boolean.class));

        Method getDescription = GUC.class.getDeclaredMethod("getDescription");
        assertThat(getDescription.getReturnType(), is(String.class));

        Method getTransformationCatalogEntry =
                GUC.class.getDeclaredMethod(
                        "getTransformationCatalogEntry", String.class, int.class);
        assertThat(
                edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry.class,
                is(getTransformationCatalogEntry.getReturnType()));

        Method generateArgumentString =
                GUC.class.getDeclaredMethod("generateArgumentString", TransferJob.class);
        assertThat(generateArgumentString.getReturnType(), is(String.class));
        assertThat(Modifier.isProtected(generateArgumentString.getModifiers()), is(true));
    }

    @Test
    public void testProtectedHelperMethodSignatures() throws Exception {
        Method defaultTCEntry =
                GUC.class.getDeclaredMethod(
                        "defaultTCEntry", String.class, String.class, String.class, String.class);
        assertThat(
                edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry.class,
                is(defaultTCEntry.getReturnType()));
        assertThat(Modifier.isProtected(defaultTCEntry.getModifiers()), is(true));

        Method getEnvironmentVariables =
                GUC.class.getDeclaredMethod("getEnvironmentVariables", String.class);
        assertThat(getEnvironmentVariables.getReturnType(), is(java.util.List.class));
        assertThat(Modifier.isProtected(getEnvironmentVariables.getModifiers()), is(true));

        Method getDerivationNamespace = GUC.class.getDeclaredMethod("getDerivationNamespace");
        assertThat(getDerivationNamespace.getReturnType(), is(String.class));

        Method getDerivationName = GUC.class.getDeclaredMethod("getDerivationName");
        assertThat(getDerivationName.getReturnType(), is(String.class));

        Method getDerivationVersion = GUC.class.getDeclaredMethod("getDerivationVersion");
        assertThat(getDerivationVersion.getReturnType(), is(String.class));
    }
}
