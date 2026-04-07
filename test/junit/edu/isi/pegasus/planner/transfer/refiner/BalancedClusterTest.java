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
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.Implementation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class BalancedClusterTest {

    @Test
    public void testBalancedClusterExtendsBasicAndConstants() {
        assertThat(BalancedCluster.class.getSuperclass(), sameInstance(Basic.class));
        assertThat(
                BalancedCluster.DESCRIPTION,
                is("Balanced Cluster Transfer Refiner( round robin distribution at file level)"));
        assertThat(
                BalancedCluster.SCALING_MESSAGE_PREFIX,
                is(
                        "Pegasus now has a strategy for scaling transfer jobs based on size of workflow."));
        assertThat(
                BalancedCluster.SCALING_MESSAGE_PROPERTY_PREFIX,
                is("Consider removing the property"));
        assertThat(
                BalancedCluster.SCALING_MESSAGE_PROFILE_PREFIX,
                is("Consider removing the pegasus profile"));
        assertThat(BalancedCluster.NUM_COMPUTE_JOBS_PER_TRANSFER_JOB, is(10.0f));
        assertThat(BalancedCluster.DEFAULT_TX_JOBS_FOR_DELETED_JOBS, is(10));
    }

    @Test
    public void testConstructorAndSelectedMethodSignatures() throws Exception {
        assertThat(
                BalancedCluster.class.getDeclaredConstructor(ADag.class, PegasusBag.class),
                notNullValue());

        Method initializeClusterValues =
                BalancedCluster.class.getDeclaredMethod("initializeClusterValues");
        Method getDefaultClusterValue =
                BalancedCluster.class.getDeclaredMethod(
                        "getDefaultClusterValueFromProperties",
                        String.class,
                        String.class,
                        int.class);
        Method addStageInDetailed =
                BalancedCluster.class.getDeclaredMethod(
                        "addStageInXFERNodes",
                        Job.class,
                        boolean.class,
                        Collection.class,
                        int.class,
                        Map.class,
                        BalancedCluster.ClusterValue.class,
                        Implementation.class);

        assertThat(Modifier.isProtected(initializeClusterValues.getModifiers()), is(true));
        assertThat(initializeClusterValues.getReturnType(), sameInstance(void.class));
        assertThat(getDefaultClusterValue.getReturnType(), sameInstance(int.class));
        assertThat(addStageInDetailed.getReturnType(), sameInstance(void.class));
    }

    @Test
    public void testDeclaredFieldTypes() throws Exception {
        Field stageInLocalMapPerLevel =
                BalancedCluster.class.getDeclaredField("mStageInLocalMapPerLevel");
        Field stageInRemoteMapPerLevel =
                BalancedCluster.class.getDeclaredField("mStageInRemoteMapPerLevel");
        Field currentSILevel = BalancedCluster.class.getDeclaredField("mCurrentSILevel");
        Field txJobsPerLevelMap = BalancedCluster.class.getDeclaredField("mTXJobsPerLevelMap");
        Field scalingMessages = BalancedCluster.class.getDeclaredField("mScalingMessages");

        assertThat(stageInLocalMapPerLevel.getType(), sameInstance(Map.class));
        assertThat(stageInRemoteMapPerLevel.getType(), sameInstance(Map.class));
        assertThat(currentSILevel.getType(), sameInstance(int.class));
        assertThat(txJobsPerLevelMap.getType(), sameInstance(Map.class));
        assertThat(scalingMessages.getType(), sameInstance(Set.class));
    }

    @Test
    public void testPrivateHelperMethodsExist() throws Exception {
        Method buildDefaultMap =
                BalancedCluster.class.getDeclaredMethod(
                        "buildDefaultTXJobsPerLevelMap", float.class);
        Method logDeferredPropertyMessage =
                BalancedCluster.class.getDeclaredMethod(
                        "logDefferedScalingPropertyMessage", String.class);

        assertThat(buildDefaultMap.getReturnType(), sameInstance(Map.class));
        assertThat(logDeferredPropertyMessage.getReturnType(), sameInstance(void.class));
        assertThat(Modifier.isPrivate(buildDefaultMap.getModifiers()), is(true));
        assertThat(Modifier.isProtected(logDeferredPropertyMessage.getModifiers()), is(true));
    }
}
