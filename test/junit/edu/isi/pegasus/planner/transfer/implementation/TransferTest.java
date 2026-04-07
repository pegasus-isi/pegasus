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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TransferTest {

    @Test
    public void testConstantsAndInheritance() {
        assertThat(Transfer.TRANSFORMATION_NAMESPACE, is("pegasus"));
        assertThat(Transfer.TRANSFORMATION_NAME, is("transfer"));
        assertThat(Transfer.TRANSFORMATION_VERSION, nullValue());
        assertThat(Transfer.DERIVATION_NAMESPACE, is("pegasus"));
        assertThat(Transfer.DERIVATION_NAME, is("transfer"));
        assertThat(Transfer.DERIVATION_VERSION, is("1.0"));
        assertThat(Transfer.DEFAULT_NUMBER_OF_THREADS, is(2));
        assertThat(Transfer.DESCRIPTION, is("Python based Transfer Script"));
        assertThat(Transfer.EXECUTABLE_BASENAME, is("pegasus-transfer"));
        assertThat(Transfer.class.getSuperclass(), is(AbstractMultipleFTPerXFERJob.class));
    }

    @Test
    public void testGetDAGManCategoryMappings() {
        assertThat(Transfer.getDAGManCategory(Job.STAGE_IN_JOB), is("stagein"));
        assertThat(Transfer.getDAGManCategory(Job.STAGE_OUT_JOB), is("stageout"));
        assertThat(Transfer.getDAGManCategory(Job.INTER_POOL_JOB), is("stageinter"));
        assertThat(Transfer.getDAGManCategory(-1), is("transfer"));
    }

    @Test
    public void testDeprecatedDAGManCategoryNames() {
        Map<String, String> first = Transfer.deprecatedDAGManCategoryNames();
        Map<String, String> second = Transfer.deprecatedDAGManCategoryNames();

        assertThat(first.size(), is(3));
        assertThat(first.get("stage-in"), is("stagein"));
        assertThat(first.get("stage-out"), is("stageout"));
        assertThat(first.get("stage-inter"), is("stageinter"));
        assertThat(first, not(sameInstance(second)));

        first.put("extra", "value");
        assertThat(second.containsKey("extra"), is(false));
    }

    @Test
    public void testConstructorAndMethodSignatures() throws Exception {
        Constructor<Transfer> constructor = Transfer.class.getConstructor(PegasusBag.class);
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));

        assertThat(
                Transfer.class.getMethod("useThirdPartyTransferAlways").getReturnType(),
                is(boolean.class));
        assertThat(Transfer.class.getMethod("doesPreserveXBit").getReturnType(), is(boolean.class));
        assertThat(Transfer.class.getMethod("getDescription").getReturnType(), is(String.class));
        assertThat(
                Transfer.class
                        .getMethod("getTransformationCatalogEntry", String.class, int.class)
                        .getReturnType()
                        .getName(),
                is("edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry"));
        assertThat(
                Transfer.class.getMethod("postProcess", TransferJob.class).getReturnType(),
                is(void.class));

        assertProtectedMethod("getEnvironmentVariables", java.util.List.class, String.class);
        assertProtectedMethod("getDerivationNamespace", String.class);
        assertProtectedMethod("getDerivationName", String.class);
        assertProtectedMethod("getDerivationVersion", String.class);
        assertProtectedMethod("generateArgumentString", String.class, TransferJob.class);
        assertProtectedMethod(
                "writeStdInAndAssociateCredentials",
                void.class,
                TransferJob.class,
                FileWriter.class,
                Collection.class,
                String.class,
                int.class);
        assertProtectedMethod("getCompleteTCName", String.class);
    }

    @Test
    public void testTransferDeclaresExpectedFieldsAndMethodCount() {
        Field[] fields = Transfer.class.getDeclaredFields();

        assertThat(fields.length, is(9));
        for (Field field : fields) {
            assertThat(Modifier.isStatic(field.getModifiers()), is(true));
            assertThat(Modifier.isFinal(field.getModifiers()), is(true));
        }

        assertThat(Transfer.class.getDeclaredMethods().length, is(14));
    }

    private void assertProtectedMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = Transfer.class.getDeclaredMethod(name, parameterTypes);
        assertThat(method.getReturnType(), is(returnType));
        assertThat(Modifier.isProtected(method.getModifiers()), is(true));
    }
}
