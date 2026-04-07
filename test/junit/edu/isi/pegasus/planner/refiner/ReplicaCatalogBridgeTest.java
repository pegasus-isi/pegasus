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
package edu.isi.pegasus.planner.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Structural tests for ReplicaCatalogBridge. */
public class ReplicaCatalogBridgeTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(ReplicaCatalogBridge.class), is(true));
    }

    @Test
    public void testOutputReplicaCatalogPrefix() {
        assertThat(
                ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX,
                is("pegasus.catalog.replica.output"));
    }

    @Test
    public void testDefaultRegistrationCategoryKey() {
        assertThat(ReplicaCatalogBridge.DEFAULT_REGISTRATION_CATEGORY_KEY, is("registration"));
    }

    @Test
    public void testRCTransformationNS() {
        assertThat(ReplicaCatalogBridge.RC_TRANSFORMATION_NS, is("pegasus"));
    }

    @Test
    public void testRCTransformationName() {
        assertThat(ReplicaCatalogBridge.RC_TRANSFORMATION_NAME, is("rc-client"));
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(ReplicaCatalogBridge.RC_DERIVATION_VERSION, is("1.0"));
        assertThat(ReplicaCatalogBridge.CACHE_REPLICA_CATALOG_IMPLEMENTER, is("SimpleFile"));
        assertThat(ReplicaCatalogBridge.DIRECTORY_REPLICA_CATALOG_IMPLEMENTER, is("Directory"));
    }

    @Test
    public void testHasDagAndPegasusBagConstructor() throws Exception {
        Constructor<ReplicaCatalogBridge> constructor =
                ReplicaCatalogBridge.class.getDeclaredConstructor(ADag.class, PegasusBag.class);
        assertThat(constructor, notNullValue());
    }

    @Test
    public void testSelectedMethodReturnTypes() throws Exception {
        Method filesMethod = ReplicaCatalogBridge.class.getMethod("getFilesInReplica");
        Method closeMethod = ReplicaCatalogBridge.class.getMethod("closeConnection");
        assertThat((Object) filesMethod.getReturnType(), is((Object) java.util.Set.class));
        assertThat((Object) closeMethod.getReturnType(), is((Object) void.class));
    }

    @Test
    public void testInitializeOverloadsExist() throws Exception {
        assertThat(
                ReplicaCatalogBridge.class.getMethod("initialize", ADag.class, PegasusBag.class),
                notNullValue());
        assertThat(
                ReplicaCatalogBridge.class.getMethod(
                        "initialize",
                        ADag.class,
                        edu.isi.pegasus.planner.common.PegasusProperties.class,
                        edu.isi.pegasus.planner.classes.PlannerOptions.class),
                notNullValue());
    }
}
