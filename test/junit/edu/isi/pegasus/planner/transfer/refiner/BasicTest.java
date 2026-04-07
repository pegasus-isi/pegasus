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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.MultipleFTPerXFERJobRefiner;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class BasicTest {

    @Test
    public void testBasicExtendsMultipleFTPerXFERJobRefinerAndDescriptionConstant() {
        assertThat(Basic.class.getSuperclass(), is(MultipleFTPerXFERJobRefiner.class));
        assertThat(Basic.DESCRIPTION, is("Default Multiple Refinement "));
    }

    @Test
    public void testConstructorAndSelectedMethodSignatures() throws Exception {
        assertThat(
                Basic.class.getDeclaredConstructor(ADag.class, PegasusBag.class),
                is(notNullValue()));

        Method addStageInSimple =
                Basic.class.getDeclaredMethod(
                        "addStageInXFERNodes", Job.class, Collection.class, Collection.class);
        Method addStageInDetailed =
                Basic.class.getDeclaredMethod(
                        "addStageInXFERNodes",
                        Job.class,
                        Collection.class,
                        String.class,
                        Implementation.class);
        Method addInterSite =
                Basic.class.getDeclaredMethod(
                        "addInterSiteTXNodes", Job.class, Collection.class, boolean.class);

        assertThat(addStageInSimple.getReturnType(), is(void.class));
        assertThat(addStageInDetailed.getReturnType(), is(void.class));
        assertThat(addInterSite.getReturnType(), is(void.class));
    }

    @Test
    public void testDeclaredFieldTypes() throws Exception {
        Field logMsg = Basic.class.getDeclaredField("mLogMsg");
        Field fileTable = Basic.class.getDeclaredField("mFileTable");
        Field relationsMap = Basic.class.getDeclaredField("mRelationsMap");
        Field createRegistrationJobs = Basic.class.getDeclaredField("mCreateRegistrationJobs");

        assertThat(logMsg.getType(), is(String.class));
        assertThat(fileTable.getType(), is(Map.class));
        assertThat(relationsMap.getType(), is(Map.class));
        assertThat(createRegistrationJobs.getType(), is(Boolean.class));
    }

    @Test
    public void testProtectedHelperMethodsExist() throws Exception {
        Method addRelation =
                Basic.class.getDeclaredMethod(
                        "addRelation", String.class, String.class, String.class, boolean.class);
        Method constructFileKey =
                Basic.class.getDeclaredMethod("constructFileKey", String.class, String.class);
        Method getJobPriority = Basic.class.getDeclaredMethod("getJobPriority", Job.class);

        assertThat(Modifier.isPublic(addRelation.getModifiers()), is(true));
        assertThat(addRelation.getReturnType(), is(void.class));
        assertThat(constructFileKey.getReturnType(), is(String.class));
        assertThat(getJobPriority.getReturnType(), is(int.class));
    }
}
