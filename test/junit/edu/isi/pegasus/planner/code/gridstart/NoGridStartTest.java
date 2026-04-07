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
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class NoGridStartTest {

    @Test
    public void testConstantsAndInitializationFreeMethods() {
        NoGridStart gridStart = new NoGridStart();

        assertThat(NoGridStart.CLASSNAME, is("NoGridStart"));
        assertThat(NoGridStart.SHORT_NAME, is("none"));
        assertThat(GridStart.class.isAssignableFrom(NoGridStart.class), is(true));
        assertThat(gridStart.canSetXBit(), is(false));
        assertThat(gridStart.canGenerateChecksumsOfOutputs(), is(false));
        assertThat(gridStart.getVDSKeyValue(), is(NoGridStart.CLASSNAME));
        assertThat(gridStart.shortDescribe(), is(NoGridStart.SHORT_NAME));
        assertThat(gridStart.defaultPOSTScript(), is(NoPOSTScript.SHORT_NAME));
        assertThat(gridStart.defaultPOSTScript(new Job()), is(NoPOSTScript.SHORT_NAME));
    }

    @Test
    public void testInitializeSetsExpectedFields() throws Exception {
        NoGridStart gridStart = new NoGridStart();
        PegasusBag bag = createBag();
        ADag dag = new ADag();

        gridStart.initialize(bag, dag);

        assertThat(getField(gridStart, "mBag"), sameInstance(bag));
        assertThat(getField(gridStart, "mDAG"), sameInstance(dag));
        assertThat(getField(gridStart, "mProps"), sameInstance(bag.getPegasusProperties()));
        assertThat(getField(gridStart, "mPOptions"), sameInstance(bag.getPlannerOptions()));
        assertThat(getField(gridStart, "mSubmitDir"), is("/submit/dir"));
        assertThat(getField(gridStart, "mUseFullPathToGridStart"), is(Boolean.TRUE));
        assertThat(getField(gridStart, "mEnablingPartOfAggregatedJob"), is(Boolean.FALSE));
    }

    @Test
    public void testUseFullPathToGridStartsUpdatesFlag() throws Exception {
        NoGridStart gridStart = new NoGridStart();
        gridStart.initialize(createBag(), new ADag());

        gridStart.useFullPathToGridStarts(false);
        assertThat(getField(gridStart, "mUseFullPathToGridStart"), is(Boolean.FALSE));

        gridStart.useFullPathToGridStarts(true);
        assertThat(getField(gridStart, "mUseFullPathToGridStart"), is(Boolean.TRUE));
    }

    @Test
    public void testSelectedFieldTypesAndMethodSignatures() throws Exception {
        assertField("mBag", PegasusBag.class, Modifier.PRIVATE);
        assertField("mDAG", ADag.class, Modifier.PRIVATE);
        assertField("mLogger", edu.isi.pegasus.common.logging.LogManager.class, Modifier.PROTECTED);
        assertField("mProps", PegasusProperties.class, Modifier.PROTECTED);
        assertField("mSubmitDir", String.class, Modifier.PROTECTED);
        assertField("mSLS", edu.isi.pegasus.planner.transfer.SLS.class, Modifier.PROTECTED);
        assertField("mUseFullPathToGridStart", boolean.class, Modifier.PRIVATE);

        assertMethod("initialize", void.class, PegasusBag.class, ADag.class);
        assertMethod("enable", AggregatedJob.class, AggregatedJob.class, Collection.class);
        assertMethod("enable", boolean.class, AggregatedJob.class, boolean.class);
        assertMethod("enable", boolean.class, Job.class, boolean.class);
        assertMethod("canSetXBit", boolean.class);
        assertMethod("getVDSKeyValue", String.class);
        assertMethod("shortDescribe", String.class);
        assertMethod("defaultPOSTScript", String.class, Job.class);
        assertMethod("defaultPOSTScript", String.class);
        assertMethod("canGenerateChecksumsOfOutputs", boolean.class);
        assertMethod("getWorkerNodeDirectory", String.class, Job.class);
        assertMethod("useFullPathToGridStarts", void.class, boolean.class);

        assertDeclaredMethod(
                "handleTransferOfExecutable", String.class, Modifier.PROTECTED, Job.class);
        assertDeclaredMethod(
                "wrapJobWithGridStartLauncher", void.class, Modifier.PROTECTED, Job.class);
        assertDeclaredMethod(
                "requiresToSetDirectory", boolean.class, Modifier.PROTECTED, Job.class);
        assertDeclaredMethod("getDirectory", String.class, Modifier.PROTECTED, Job.class);
        assertDeclaredMethod(
                "generateListofFilenamesFile",
                String.class,
                Modifier.PROTECTED,
                Set.class,
                Job.class,
                String.class);
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory("/submit/dir");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        return bag;
    }

    private Object getField(Object instance, String name) throws Exception {
        Field field = NoGridStart.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private void assertField(String name, Class<?> type, int requiredModifier) throws Exception {
        Field field = NoGridStart.class.getDeclaredField(name);
        assertThat((Object) field.getType(), is((Object) type));
        assertThat((field.getModifiers() & requiredModifier) != 0, is(true));
    }

    private void assertMethod(String name, Class<?> returnType, Class<?>... parameterTypes)
            throws Exception {
        Method method = NoGridStart.class.getMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
    }

    private void assertDeclaredMethod(
            String name, Class<?> returnType, int requiredModifier, Class<?>... parameterTypes)
            throws Exception {
        Method method = NoGridStart.class.getDeclaredMethod(name, parameterTypes);
        assertThat((Object) method.getReturnType(), is((Object) returnType));
        assertThat((method.getModifiers() & requiredModifier) != 0, is(true));
    }
}
