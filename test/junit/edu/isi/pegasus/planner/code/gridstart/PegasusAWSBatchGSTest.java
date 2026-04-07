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
package edu.isi.pegasus.planner.code.gridstart;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusAWSBatchGSTest {

    @Test
    public void testConstantsAndImplementsGridStart() {
        assertThat(PegasusAWSBatchGS.TRANSFER_INPUT_FILES_KEY, is("TRANSFER_INPUT_FILES"));
        assertThat(PegasusAWSBatchGS.CLASSNAME, is("PegasusAWSBatchGS"));
        assertThat(PegasusAWSBatchGS.SHORT_NAME, is("pegasus-aws-batch"));
        assertThat(PegasusAWSBatchGS.SEPARATOR, is("########################"));
        assertThat(PegasusAWSBatchGS.SEPARATOR_CHAR, is('#'));
        assertThat(PegasusAWSBatchGS.MESSAGE_PREFIX, is("[Pegasus AWS Batch Gridstart] "));
        assertThat(PegasusAWSBatchGS.MESSAGE_STRING_LENGTH, is(80));
        assertThat(GridStart.class.isAssignableFrom(PegasusAWSBatchGS.class), is(true));
    }

    @Test
    public void testConstructorAndInitializationFreeMethods() throws Exception {
        PegasusAWSBatchGS gridStart = new PegasusAWSBatchGS();

        assertThat(gridStart.getVDSKeyValue(), is(PegasusAWSBatchGS.CLASSNAME));
        assertThat(gridStart.shortDescribe(), is(PegasusAWSBatchGS.SHORT_NAME));
        assertThat(gridStart.canSetXBit(), is(false));

        Field pegasusLiteField = PegasusAWSBatchGS.class.getDeclaredField("mPegasusLite");
        pegasusLiteField.setAccessible(true);
        assertThat(pegasusLiteField.get(gridStart), instanceOf(PegasusLite.class));
    }

    @Test
    public void testPrivateConstructAndRewriteValueHelpers() throws Exception {
        PegasusAWSBatchGS gridStart = new PegasusAWSBatchGS();
        Job job = new Job();

        Method construct =
                PegasusAWSBatchGS.class.getDeclaredMethod(
                        "construct", Job.class, String.class, String.class);
        construct.setAccessible(true);
        construct.invoke(gridStart, job, "priority", "10");

        assertThat(job.condorVariables.get("priority"), is("10"));

        Method rewriteValue =
                PegasusAWSBatchGS.class.getDeclaredMethod(
                        "rewriteValue", String.class, String.class);
        rewriteValue.setAccessible(true);

        assertThat(
                (String) rewriteValue.invoke(gridStart, "credentials.conf", "s3://bucket"),
                is("s3://bucket/credentials.conf"));
    }

    @Test
    public void testSelectedFieldTypesAndMethodSignatures() throws Exception {
        assertField("mBag", PegasusBag.class, Modifier.PRIVATE);
        assertField("mDAG", ADag.class, Modifier.PRIVATE);
        assertField("mPegasusLite", PegasusLite.class, Modifier.PRIVATE);
        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PRIVATE);
        assertField("mSubmitDir", String.class, Modifier.PRIVATE);
        assertField("mStyleFactory", CondorStyleFactory.class, Modifier.PRIVATE);
        assertField("mCurrentClusteredJobCredentials", java.util.Set.class, Modifier.PUBLIC);

        assertMethod("initialize", void.class, PegasusBag.class, ADag.class);
        assertMethod("enable", boolean.class, AggregatedJob.class, boolean.class);
        assertMethod("enable", boolean.class, Job.class, boolean.class);
        assertMethod("useFullPathToGridStarts", void.class, boolean.class);
        assertMethod("canSetXBit", boolean.class);
        assertMethod("getWorkerNodeDirectory", String.class, Job.class);
        assertMethod("getVDSKeyValue", String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("defaultPOSTScript", String.class, Job.class);
        assertMethod("defaultPOSTScript", String.class);
        assertMethod("canGenerateChecksumsOfOutputs", boolean.class);

        assertDeclaredMethod(
                "construct", void.class, Modifier.PRIVATE, Job.class, String.class, String.class);
        assertDeclaredMethod(
                "updateJobEnvForCredentials",
                String.class,
                Modifier.PRIVATE,
                Job.class,
                String.class);
        assertDeclaredMethod(
                "rewriteValue", String.class, Modifier.PRIVATE, String.class, String.class);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = PegasusAWSBatchGS.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusAWSBatchGS.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }

    private void assertDeclaredMethod(
            String name, Class<?> returnType, int requiredModifier, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusAWSBatchGS.class.getDeclaredMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
        assertThat((method.getModifiers() & requiredModifier) != 0, is(true));
    }
}
