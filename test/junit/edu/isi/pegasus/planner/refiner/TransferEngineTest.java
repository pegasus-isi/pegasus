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
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerCache;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Structural tests for TransferEngine. */
public class TransferEngineTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(TransferEngine.class), is(true));
    }

    @Test
    public void testDeletedJobsLevel() {
        assertThat(TransferEngine.DELETED_JOBS_LEVEL, is(1000));
    }

    @Test
    public void testWorkflowCacheFileImplementor() {
        assertThat(TransferEngine.WORKFLOW_CACHE_FILE_IMPLEMENTOR, is("FlushedCache"));
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(TransferEngine.WORKFLOW_CACHE_REPLICA_CATALOG_KEY, is("file"));
        assertThat(TransferEngine.REFINER_NAME, is("TranferEngine"));
    }

    @Test
    public void testHasExpectedConstructor() throws Exception {
        Constructor<TransferEngine> constructor =
                TransferEngine.class.getDeclaredConstructor(
                        ADag.class, PegasusBag.class, List.class, List.class);
        assertThat(constructor, notNullValue());
    }

    @Test
    public void testAddTransferNodesReturnsVoid() throws Exception {
        Method method =
                TransferEngine.class.getMethod(
                        "addTransferNodes", ReplicaCatalogBridge.class, PlannerCache.class);
        assertThat((Object) method.getReturnType(), is((Object) void.class));
    }

    @Test
    public void testGetStagingSiteReturnsString() throws Exception {
        Method method = TransferEngine.class.getMethod("getStagingSite", Job.class);
        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }
}
