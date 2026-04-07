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
package edu.isi.pegasus.planner.transfer.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class BundleTest {

    @Test
    public void testBundleExtendsBasicAndConstants() {
        assertThat(Bundle.class.getSuperclass(), sameInstance(Basic.class));
        assertThat(
                Bundle.DESCRIPTION, is("Bundle Mode (stagein files distributed amongst bundles)"));
        assertThat(Bundle.DEFAULT_LOCAL_STAGE_IN_BUNDLE_FACTOR, is(2));
        assertThat(Bundle.DEFAULT_REMOTE_STAGE_IN_BUNDLE_FACTOR, is(2));
        assertThat(Bundle.DEFAULT_LOCAL_STAGE_OUT_BUNDLE_FACTOR, is(2));
        assertThat(Bundle.DEFAULT_REMOTE_STAGE_OUT_BUNDLE_FACTOR, is(2));
        assertThat(Bundle.NO_PROFILE_VALUE, is(-1));
    }

    @Test
    public void testConstructorAndDeclaredFieldTypes() throws Exception {
        Field localMapField = Bundle.class.getDeclaredField("mStageInLocalMap");
        assertThat(localMapField.getType(), sameInstance(Map.class));

        Field remoteMapField = Bundle.class.getDeclaredField("mStageInRemoteMap");
        assertThat(remoteMapField.getType(), sameInstance(Map.class));

        Field relationsParentMapField = Bundle.class.getDeclaredField("mRelationsParentMap");
        assertThat(relationsParentMapField.getType(), sameInstance(Map.class));

        Field setupMapField = Bundle.class.getDeclaredField("mSetupMap");
        assertThat(setupMapField.getType(), sameInstance(Map.class));

        Field currentSOLevelField = Bundle.class.getDeclaredField("mCurrentSOLevel");
        assertThat(currentSOLevelField.getType(), sameInstance(int.class));

        Field jobPrefixField = Bundle.class.getDeclaredField("mJobPrefix");
        assertThat(jobPrefixField.getType(), sameInstance(String.class));

        Field siteStoreField = Bundle.class.getDeclaredField("mSiteStore");
        assertThat(
                siteStoreField.getType().getName(),
                is("edu.isi.pegasus.planner.catalog.site.classes.SiteStore"));

        Field pegasusProfilesField = Bundle.class.getDeclaredField("mPegasusProfilesInProperties");
        assertThat(
                pegasusProfilesField.getType().getName(),
                is("edu.isi.pegasus.planner.namespace.Pegasus"));

        assertThat(
                Bundle.class.getDeclaredConstructor(ADag.class, PegasusBag.class), notNullValue());
    }

    @Test
    public void testProtectedHelperMethodSignatures() throws Exception {
        Method method =
                Bundle.class.getDeclaredMethod(
                        "getDefaultBundleValueFromProperties",
                        String.class,
                        String.class,
                        int.class);

        assertThat(method.getReturnType(), sameInstance(int.class));
        assertThat(Modifier.isProtected(method.getModifiers()), is(true));
        assertThat(method.getParameterCount(), is(3));
    }

    @Test
    public void testDescriptionAndPoolTransferStructure() throws Exception {
        Method descriptionMethod = Bundle.class.getDeclaredMethod("getDescription");
        assertThat(descriptionMethod.getReturnType(), sameInstance(String.class));
        assertThat(Modifier.isPublic(descriptionMethod.getModifiers()), is(true));

        Method poolTransferMethod =
                Bundle.class.getDeclaredMethod(
                        "getStageOutPoolTransfer", String.class, boolean.class, int.class);
        assertThat(
                poolTransferMethod.getReturnType().getName(),
                is("edu.isi.pegasus.planner.transfer.refiner.Bundle$PoolTransfer"));
        assertThat(Modifier.isPublic(poolTransferMethod.getModifiers()), is(true));

        Class<?>[] innerClasses = Bundle.class.getDeclaredClasses();
        boolean hasPoolTransfer = false;
        boolean hasBundleValue = false;
        for (Class<?> innerClass : innerClasses) {
            if (innerClass.getSimpleName().equals("PoolTransfer")) {
                hasPoolTransfer = true;
                assertThat(Modifier.isProtected(innerClass.getModifiers()), is(true));
            }
            if (innerClass.getSimpleName().equals("BundleValue")) {
                hasBundleValue = true;
                assertThat(Modifier.isProtected(innerClass.getModifiers()), is(true));
            }
        }

        assertThat(hasPoolTransfer, is(true));
        assertThat(hasBundleValue, is(true));
    }
}
