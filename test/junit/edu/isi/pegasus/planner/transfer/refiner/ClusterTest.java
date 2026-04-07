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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.Implementation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ClusterTest {

    @Test
    public void testClusterExtendsBundleAndConstants() {
        assertThat(Cluster.class.getSuperclass(), is(Bundle.class));
        assertThat(
                Cluster.DESCRIPTION,
                is("Cluster Transfers: Stagein and Stageout TX jobs are clustered per level"));
        assertThat(Cluster.NUM_COMPUTE_JOBS_PER_TRANSFER_JOB, is(10.0f));
        assertThat(Cluster.DEFAULT_TX_JOBS_FOR_DELETED_JOBS, is(10));
    }

    @Test
    public void testConstructorAndSelectedMethodSignatures() throws Exception {
        assertThat(
                Cluster.class.getDeclaredConstructor(ADag.class, PegasusBag.class), notNullValue());

        Method initializeBundleValues = Cluster.class.getDeclaredMethod("initializeBundleValues");
        Method addStageInSimple =
                Cluster.class.getDeclaredMethod(
                        "addStageInXFERNodes", Job.class, Collection.class, Collection.class);
        Method addStageInDetailed =
                Cluster.class.getDeclaredMethod(
                        "addStageInXFERNodes",
                        Job.class,
                        boolean.class,
                        Collection.class,
                        int.class,
                        Map.class,
                        Bundle.BundleValue.class,
                        Implementation.class);

        assertThat(Modifier.isProtected(initializeBundleValues.getModifiers()), is(true));
        assertThat(initializeBundleValues.getReturnType(), is(void.class));
        assertThat(addStageInSimple.getReturnType(), is(void.class));
        assertThat(addStageInDetailed.getReturnType(), is(void.class));
    }

    @Test
    public void testDeclaredFieldTypes() throws Exception {
        Field stageInLocalMapPerLevel = Cluster.class.getDeclaredField("mStageInLocalMapPerLevel");
        Field stageInRemoteMapPerLevel =
                Cluster.class.getDeclaredField("mStageInRemoteMapPerLevel");
        Field currentSILevel = Cluster.class.getDeclaredField("mCurrentSILevel");
        Field syncJobMap = Cluster.class.getDeclaredField("mSyncJobMap");
        Field txJobsPerLevelMap = Cluster.class.getDeclaredField("mTXJobsPerLevelMap");

        assertThat(stageInLocalMapPerLevel.getType(), is(Map.class));
        assertThat(stageInRemoteMapPerLevel.getType(), is(Map.class));
        assertThat(currentSILevel.getType(), is(int.class));
        assertThat(syncJobMap.getType(), is(Map.class));
        assertThat(txJobsPerLevelMap.getType(), is(Map.class));
    }

    @Test
    public void testPrivateHelperMethodsExist() throws Exception {
        Method buildDefaultMap =
                Cluster.class.getDeclaredMethod("buildDefaultTXJobsPerLevelMap", float.class);
        Method getSyncJob = Cluster.class.getDeclaredMethod("getSyncJob", String.class);

        assertThat(buildDefaultMap.getReturnType(), is(Map.class));
        assertThat(getSyncJob.getReturnType(), is(Job.class));
        assertThat(Modifier.isPrivate(buildDefaultMap.getModifiers()), is(true));
        assertThat(Modifier.isPublic(getSyncJob.getModifiers()), is(true));
    }
}
