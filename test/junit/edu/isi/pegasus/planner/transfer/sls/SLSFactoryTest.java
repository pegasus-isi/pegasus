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
package edu.isi.pegasus.planner.transfer.sls;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.transfer.SLS;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class SLSFactoryTest {

    @Test
    public void testConstantsAndConstructorDefaults() throws Exception {
        assertThat(SLSFactory.DEFAULT_PACKAGE_NAME, is("edu.isi.pegasus.planner.transfer.sls"));
        assertThat(SLSFactory.DEFAULT_SLS_IMPL_CLASS, is("Transfer"));
        assertThat(SLSFactory.CONDORIO_SLS_IMPL_CLASS, is("Condor"));
        assertThat(SLSFactory.SLS_IMPLEMENTING_CLASSES, arrayContaining("Transfer", "Condor"));

        SLSFactory factory = new SLSFactory();

        assertThat(getInitialized(factory), is(false));
        assertThat(getImplementationTable(factory).isEmpty(), is(true));
        assertThat(getBag(factory), nullValue());
    }

    @Test
    public void testPrivateGetSLSShortNameMappings() throws Exception {
        SLSFactory factory = new SLSFactory();
        Method method = getSLSShortNameMethod();

        Job defaultJob = new Job();
        assertThat((String) method.invoke(factory, defaultJob), is("Transfer"));

        Job condorJob = new Job();
        condorJob.setDataConfiguration(PegasusConfiguration.CONDOR_CONFIGURATION_VALUE);
        assertThat((String) method.invoke(factory, condorJob), is("Condor"));
    }

    @Test
    public void testPrivateLoadInstanceRejectsNullProperties() throws Exception {
        SLSFactory factory = new SLSFactory();
        PegasusBag bag = new PegasusBag();

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                getLoadInstanceMethod()
                                        .invoke(factory, bag, TestSLS.class.getName()));

        assertThat(exception.getCause().getClass(), sameInstance(SLSFactoryException.class));
        SLSFactoryException wrapped = (SLSFactoryException) exception.getCause();
        assertThat(wrapped.getMessage(), is("Invalid properties passed"));
        assertThat(wrapped.getClassname(), is(SLSFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testPrivateLoadInstanceRejectsNullClassName() throws Exception {
        SLSFactory factory = new SLSFactory();
        PegasusBag bag = createBagWithProperties();

        InvocationTargetException exception =
                assertThrows(
                        InvocationTargetException.class,
                        () -> getLoadInstanceMethod().invoke(factory, bag, null));

        assertThat(exception.getCause().getClass(), sameInstance(SLSFactoryException.class));
        SLSFactoryException wrapped = (SLSFactoryException) exception.getCause();
        assertThat(wrapped.getMessage(), is("Invalid className specified"));
        assertThat(wrapped.getClassname(), is(SLSFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testPrivateLoadInstanceWithExplicitClassNameInitializesImplementation()
            throws Exception {
        SLSFactory factory = new SLSFactory();
        PegasusBag bag = createBagWithProperties();
        TestSLS.reset();

        SLS sls = (SLS) getLoadInstanceMethod().invoke(factory, bag, TestSLS.class.getName());

        assertThat(sls, notNullValue());
        assertThat(sls.getClass(), sameInstance(TestSLS.class));
        assertThat(TestSLS.initializeCalled, is(true));
        assertThat(TestSLS.lastBag, sameInstance(bag));
    }

    @Test
    public void testLoadInstanceRejectsUseBeforeInitialize() {
        SLSFactory factory = new SLSFactory();

        SLSFactoryException exception =
                assertThrows(SLSFactoryException.class, () -> factory.loadInstance(new Job()));

        assertThat(
                exception.getMessage(),
                is("SLSFactory needs to be initialized first before using"));
    }

    @Test
    public void testLoadInstanceReturnsCachedImplementation() throws Exception {
        SLSFactory factory = new SLSFactory();
        HashMap table = getImplementationTable(factory);
        TestSLS cached = new TestSLS();
        table.put("Transfer", cached);
        setInitialized(factory, true);

        Job job = new Job();
        job.setDataConfiguration("non-condor");

        SLS sls = factory.loadInstance(job);

        assertThat(sls, sameInstance(cached));
    }

    private PegasusBag createBagWithProperties() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        return bag;
    }

    private Method getLoadInstanceMethod() throws Exception {
        Method method =
                SLSFactory.class.getDeclaredMethod("loadInstance", PegasusBag.class, String.class);
        method.setAccessible(true);
        return method;
    }

    private Method getSLSShortNameMethod() throws Exception {
        Method method = SLSFactory.class.getDeclaredMethod("getSLSShortName", Job.class);
        method.setAccessible(true);
        return method;
    }

    private HashMap getImplementationTable(SLSFactory factory) throws Exception {
        return (HashMap) ReflectionTestUtils.getField(factory, "mSLSImplementationTable");
    }

    private boolean getInitialized(SLSFactory factory) throws Exception {
        return (Boolean) ReflectionTestUtils.getField(factory, "mInitialized");
    }

    private void setInitialized(SLSFactory factory, boolean value) throws Exception {
        ReflectionTestUtils.setField(factory, "mInitialized", value);
    }

    private PegasusBag getBag(SLSFactory factory) throws Exception {
        return (PegasusBag) ReflectionTestUtils.getField(factory, "mBag");
    }

    public static class TestSLS implements SLS {

        static boolean initializeCalled;
        static PegasusBag lastBag;

        static void reset() {
            initializeCalled = false;
            lastBag = null;
        }

        @Override
        public void initialize(PegasusBag bag) {
            initializeCalled = true;
            lastBag = bag;
        }

        @Override
        public boolean doesCondorModifications() {
            return false;
        }

        @Override
        public String invocationString(Job job, File slsFile) {
            return null;
        }

        @Override
        public boolean needsSLSInputTransfers(Job job) {
            return false;
        }

        @Override
        public boolean needsSLSOutputTransfers(Job job) {
            return false;
        }

        @Override
        public String getSLSInputLFN(Job job) {
            return null;
        }

        @Override
        public String getSLSOutputLFN(Job job) {
            return null;
        }

        @Override
        public Collection<FileTransfer> determineSLSInputTransfers(
                Job job,
                String fileName,
                FileServer stagingSiteServer,
                String stagingSiteDirectory,
                String workerNodeDirectory,
                boolean onlyContainer) {
            return null;
        }

        @Override
        public Collection<FileTransfer> determineSLSOutputTransfers(
                Job job,
                String fileName,
                FileServer stagingSiteServer,
                String stagingSiteDirectory,
                String workerNodeDirectory) {
            return null;
        }

        @Override
        public boolean modifyJobForWorkerNodeExecution(
                Job job,
                String stagingSiteURLPrefix,
                String stagingSitedirectory,
                String workerNodeDirectory) {
            return false;
        }

        @Override
        public String getDescription() {
            return "test";
        }
    }
}
