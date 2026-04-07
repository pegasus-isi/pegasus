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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import edu.isi.pegasus.planner.transfer.implementation.TransferImplementationFactoryException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class RefinerTest {

    @Test
    public void testRefinerIsInterfaceAndExtendsPlannerRefiner() {
        assertThat(Refiner.class.isInterface(), is(true));
        assertThat(Refiner.class.getInterfaces().length, equalTo(1));
        assertThat(
                Refiner.class.getInterfaces()[0],
                sameInstance(edu.isi.pegasus.planner.refiner.Refiner.class));
    }

    @Test
    public void testConstants() {
        assertThat(Refiner.VERSION, equalTo("1.4"));
        assertThat(Refiner.LOCAL_PREFIX, equalTo("local_"));
        assertThat(Refiner.REMOTE_PREFIX, equalTo("remote_"));
        assertThat(Refiner.STAGE_IN_PREFIX, equalTo("stage_in_"));
        assertThat(Refiner.STAGE_OUT_PREFIX, equalTo("stage_out_"));
        assertThat(Refiner.INTER_POOL_PREFIX, equalTo("stage_inter_"));
        assertThat(Refiner.REGISTER_PREFIX, equalTo("register_"));
    }

    @Test
    public void testSelectedMethodReturnTypes() throws Exception {
        Method loadImplementations =
                Refiner.class.getDeclaredMethod("loadImplementations", PegasusBag.class);
        assertThat(loadImplementations.getReturnType(), equalTo(void.class));
        assertThat(
                loadImplementations.getExceptionTypes()[0],
                equalTo(TransferImplementationFactoryException.class));

        assertThat(
                Refiner.class
                        .getDeclaredMethod(
                                "addInterSiteTXNodes", Job.class, Collection.class, boolean.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod(
                                "addStageOutXFERNodes",
                                Job.class,
                                Collection.class,
                                ReplicaCatalogBridge.class,
                                boolean.class,
                                boolean.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod(
                                "addStageOutXFERNodes",
                                Job.class,
                                Collection.class,
                                Collection.class,
                                ReplicaCatalogBridge.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod(
                                "addStageInXFERNodes",
                                Job.class,
                                Collection.class,
                                Collection.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("refinerPreferenceForTransferJobLocation")
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("refinerPreferenceForLocalTransferJobs", int.class)
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("runTransferRemotely", String.class, int.class)
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("isSiteThirdParty", String.class, int.class)
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("runTPTOnRemoteSite", String.class, int.class)
                        .getReturnType(),
                equalTo(boolean.class));
        assertThat(
                Refiner.class.getDeclaredMethod("addJob", Job.class).getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod("addRelation", String.class, String.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class
                        .getDeclaredMethod(
                                "addRelation",
                                String.class,
                                String.class,
                                String.class,
                                boolean.class)
                        .getReturnType(),
                equalTo(void.class));
        assertThat(
                Refiner.class.getDeclaredMethod("getDescription").getReturnType(),
                equalTo(String.class));
        assertThat(Refiner.class.getDeclaredMethod("done").getReturnType(), equalTo(void.class));
    }

    @Test
    public void testRefinerDeclaresExpectedMethods() {
        Method[] methods = Refiner.class.getDeclaredMethods();
        assertThat(methods.length, equalTo(15));
        for (Method method : methods) {
            assertThat(Modifier.isPublic(method.getModifiers()), is(true));
            assertThat(Modifier.isAbstract(method.getModifiers()), is(true));
        }
    }
}
