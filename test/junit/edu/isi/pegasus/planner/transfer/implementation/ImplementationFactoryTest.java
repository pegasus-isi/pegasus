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

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.Refiner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ImplementationFactoryTest {

    @Test
    public void testConstants() {
        assertThat(
                ImplementationFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.transfer.implementation"));
        assertThat(ImplementationFactory.DEFAULT_TRANSFER_IMPLEMENTATION, is("Transfer"));
        assertThat(ImplementationFactory.DEFAULT_SETUP_TRANSFER_IMPLEMENTATION, is("Transfer"));
        assertThat(ImplementationFactory.TYPE_STAGE_IN, is(0));
        assertThat(ImplementationFactory.TYPE_STAGE_INTER, is(1));
        assertThat(ImplementationFactory.TYPE_STAGE_OUT, is(2));
        assertThat(ImplementationFactory.TYPE_SETUP, is(3));
        assertThat(ImplementationFactory.TYPE_SYMLINK_STAGE_IN, is(4));
    }

    @Test
    public void testGetPropertyKeyMappings() throws Exception {
        assertThat(
                invokeGetPropertyKey(ImplementationFactory.TYPE_STAGE_IN),
                is("pegasus.transfer.stagein.impl"));
        assertThat(
                invokeGetPropertyKey(ImplementationFactory.TYPE_STAGE_INTER),
                is("pegasus.transfer.inter.impl"));
        assertThat(
                invokeGetPropertyKey(ImplementationFactory.TYPE_STAGE_OUT),
                is("pegasus.transfer.stageout.impl"));
        assertThat(
                invokeGetPropertyKey(ImplementationFactory.TYPE_SETUP),
                is("pegasus.transfer.setup.impl"));
        assertThat(
                invokeGetPropertyKey(ImplementationFactory.TYPE_SYMLINK_STAGE_IN),
                is("pegasus.transfer.symlink.impl"));
    }

    @Test
    public void testGetPropertyKeyRejectsInvalidType() throws Exception {
        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> getPropertyKeyMethod().invoke(null, 99));

        assertThat(exception.getCause().getClass(), is(IllegalArgumentException.class));
        assertThat(
                exception.getCause().getMessage(),
                is("Invalid implementation type passed to factory 99"));
    }

    @Test
    public void testPrivateLoadInstanceWithExplicitClassNameLoadsImplementation() throws Exception {
        TestImplementation.reset();
        PegasusBag bag = createBagWithProperties();

        Implementation implementation = invokeLoadInstance(TestImplementation.class.getName(), bag);

        assertThat(implementation, notNullValue());
        assertThat(implementation.getClass(), is(TestImplementation.class));
        assertThat(((TestImplementation) implementation).mBag, sameInstance(bag));
        assertThat(TestImplementation.lastBag, sameInstance(bag));
    }

    @Test
    public void testPublicLoadInstanceUsesConfiguredShortClassName() throws Exception {
        PegasusBag bag = createBagWithProperties();
        bag.getPegasusProperties()
                .setProperty(
                        "pegasus.transfer.stagein.impl", TestImplementation.class.getSimpleName());

        TransferImplementationFactoryException exception =
                assertThrows(
                        TransferImplementationFactoryException.class,
                        () ->
                                ImplementationFactory.loadInstance(
                                        bag, ImplementationFactory.TYPE_STAGE_IN));

        assertThat(
                exception.getClassname(),
                is("edu.isi.pegasus.planner.transfer.implementation.TestImplementation"));
        assertThat(exception.getCause(), notNullValue());
    }

    @Test
    public void testLoadInstanceDefaultsToTransferWhenPropertyMissing() {
        PegasusBag bag = createBagWithProperties();
        Implementation implementation =
                ImplementationFactory.loadInstance(bag, ImplementationFactory.TYPE_STAGE_IN);
        assertThat(implementation, notNullValue());
        assertThat(implementation.getClass(), is(Transfer.class));
    }

    @Test
    public void testPrivateLoadInstanceRejectsBagWithoutProperties() throws Exception {
        PegasusBag bag = new PegasusBag();

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                loadInstanceMethod()
                                        .invoke(null, TestImplementation.class.getName(), bag));

        assertThat(
                exception.getCause().getClass(), is(TransferImplementationFactoryException.class));
        TransferImplementationFactoryException wrapped =
                (TransferImplementationFactoryException) exception.getCause();
        assertThat(wrapped.getMessage(), is("Instantiating Transfer Impelmentation "));
        assertThat(wrapped.getClassname(), is(TestImplementation.class.getName()));
        assertThat(wrapped.getCause(), notNullValue());
        assertThat(wrapped.getCause().getMessage(), is("Invalid properties passed"));
    }

    private String invokeGetPropertyKey(int type) throws Exception {
        return (String) getPropertyKeyMethod().invoke(null, type);
    }

    private Method getPropertyKeyMethod() throws Exception {
        Method method = ImplementationFactory.class.getDeclaredMethod("getPropertyKey", int.class);
        method.setAccessible(true);
        return method;
    }

    private Implementation invokeLoadInstance(String className, PegasusBag bag) throws Exception {
        return (Implementation) loadInstanceMethod().invoke(null, className, bag);
    }

    private Method loadInstanceMethod() throws Exception {
        Method method =
                ImplementationFactory.class.getDeclaredMethod(
                        "loadInstance", String.class, PegasusBag.class);
        method.setAccessible(true);
        return method;
    }

    private PegasusBag createBagWithProperties() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        return bag;
    }

    public static class TestImplementation implements Implementation {

        static PegasusBag lastBag;

        final PegasusBag mBag;

        public TestImplementation(PegasusBag bag) {
            this.mBag = bag;
            lastBag = bag;
        }

        static void reset() {
            lastBag = null;
        }

        @Override
        public void setRefiner(Refiner refiner) {}

        @Override
        public TransferJob createTransferJob(
                Job job,
                String site,
                Collection files,
                Collection execFiles,
                String txJobName,
                int jobClass) {
            return null;
        }

        @Override
        public boolean doesPreserveXBit() {
            return false;
        }

        @Override
        public boolean addSetXBitJobs(
                Job computeJob,
                String txJobName,
                Collection execFiles,
                int transferClass,
                int xbitIndex) {
            return false;
        }

        @Override
        public Job createSetXBitJob(
                Job computeJob,
                Collection<FileTransfer> execFiles,
                int transferClass,
                int xbitIndex) {
            return null;
        }

        @Override
        public String getSetXBitJobName(String name, int counter) {
            return null;
        }

        @Override
        public TransformationCatalogEntry getTransformationCatalogEntry(
                String siteHandle, int jobClass) {
            return null;
        }

        @Override
        public boolean useThirdPartyTransferAlways() {
            return false;
        }

        @Override
        public void applyPriority(TransferJob job) {}

        @Override
        public String getDescription() {
            return "test";
        }
    }
}
