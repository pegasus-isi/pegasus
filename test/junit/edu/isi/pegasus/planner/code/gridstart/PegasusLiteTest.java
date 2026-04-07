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
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapperFactory;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusLiteTest {

    @Test
    public void testConstantsAndImplementsGridStart() {
        assertThat(PegasusLite.SEPARATOR, is("########################"));
        assertThat(PegasusLite.SEPARATOR_CHAR, is('#'));
        assertThat(PegasusLite.MESSAGE_PREFIX, is("[Pegasus Lite]"));
        assertThat(PegasusLite.MESSAGE_STRING_LENGTH, is(80));
        assertThat(PegasusLite.CLASSNAME, is("PegasusLite"));
        assertThat(PegasusLite.SHORT_NAME, is("pegasus-lite"));
        assertThat(PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME, is("pegasus-lite-common.sh"));
        assertThat(PegasusLite.XBIT_TRANSFORMATION, is("chmod"));
        assertThat(PegasusLite.XBIT_EXECUTABLE_BASENAME, is("chmod"));
        assertThat(PegasusLite.XBIT_TRANSFORMATION_NS, is("system"));
        assertThat(PegasusLite.XBIT_TRANSFORMATION_VERSION, nullValue());
        assertThat(PegasusLite.XBIT_DERIVATION_NS, is("system"));
        assertThat(PegasusLite.XBIT_DERIVATION_VERSION, nullValue());
        assertThat(
                PegasusLite.PEGASUS_LITE_EXITCODE_SUCCESS_MESSAGE, is("PegasusLite: exitcode 0"));
        assertThat(PegasusLite.WORKER_NODE_DIRECTORY_KEY, is("PEGASUS_WN_TMP"));
        assertThat(PegasusLite.PEGASUS_LITE_LOG_ENV_KEY, is("pegasus_lite_log_file"));
        assertThat(GridStart.class.isAssignableFrom(PegasusLite.class), is(true));
    }

    @Test
    public void testNoArgConstructionAndInitializationFreeMethods() {
        PegasusLite pegasusLite = new PegasusLite();

        assertThat(pegasusLite.getVDSKeyValue(), is(PegasusLite.CLASSNAME));
        assertThat(pegasusLite.shortDescribe(), is(PegasusLite.SHORT_NAME));
        assertThat(pegasusLite.canSetXBit(), is(false));
    }

    @Test
    public void testSelectedFieldTypesExist() throws Exception {
        assertField("mBag", PegasusBag.class, Modifier.PRIVATE);
        assertField("mDAG", ADag.class, Modifier.PRIVATE);
        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField(
                "mProps",
                edu.isi.pegasus.planner.common.PegasusProperties.class,
                Modifier.PROTECTED);
        assertField("mSubmitDir", String.class, Modifier.PROTECTED);
        assertField(
                "mSLSFactory",
                edu.isi.pegasus.planner.transfer.sls.SLSFactory.class,
                Modifier.PROTECTED);
        assertField("mContainerWrapper", ContainerShellWrapper.class, Modifier.PROTECTED);
        assertField(
                "mContainerWrapperFactory", ContainerShellWrapperFactory.class, Modifier.PROTECTED);
        assertField("mWorkerPackageMap", java.util.Map.class, 0);
    }

    @Test
    public void testConstructorAndMethodSignatures() throws Exception {
        Constructor<PegasusLite> constructor = PegasusLite.class.getConstructor();
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));

        assertMethod("initialize", void.class, PegasusBag.class, ADag.class);
        assertMethod("enable", boolean.class, AggregatedJob.class, boolean.class);
        assertMethod("enable", boolean.class, Job.class, boolean.class);
        assertMethod("canGenerateChecksumsOfOutputs", boolean.class);
        assertMethod("canSetXBit", boolean.class);
        assertMethod("getVDSKeyValue", String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("defaultPOSTScript", String.class, Job.class);
        assertMethod("defaultPOSTScript", String.class);
        assertMethod("generateListofFilenamesFile", String.class, Set.class, String.class);
        assertMethod("getWorkerNodeDirectory", String.class, Job.class);
        assertMethod("useFullPathToGridStarts", void.class, boolean.class);
        assertMethod("associateSetupScriptWithJob", boolean.class, Job.class);
        assertMethod("ignoreNullWorkerPackageLocationOnSubmitHost", boolean.class, Job.class);

        assertDeclaredMethod(
                "wrapJobWithPegasusLite",
                File.class,
                Modifier.PROTECTED,
                Job.class,
                boolean.class,
                boolean.class);
        assertDeclaredMethod(
                "convertToTransferInputFormat",
                StringBuffer.class,
                Modifier.PROTECTED,
                java.util.Collection.class,
                PegasusFile.LINKAGE.class);
        assertDeclaredMethod("setXBitOnFile", boolean.class, Modifier.PROTECTED, String.class);
        assertDeclaredMethod(
                "getSubmitHostPathToPegasusLiteCommon", String.class, Modifier.PROTECTED);
        assertDeclaredMethod(
                "retrieveLocationForWorkerPackageFromTC",
                String.class,
                Modifier.PROTECTED,
                String.class);
        assertDeclaredMethod(
                "enforceStrictChecksForWorkerPackage",
                boolean.class,
                Modifier.PROTECTED,
                PegasusBag.class);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = PegasusLite.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        if (requiredModifier != 0) {
            assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
        }
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusLite.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }

    private void assertDeclaredMethod(
            String name, Class<?> returnType, int requiredModifier, Class<?>... parameterTypes)
            throws Exception {
        Method method = PegasusLite.class.getDeclaredMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
        assertThat((method.getModifiers() & requiredModifier) != 0, is(true));
    }
}
