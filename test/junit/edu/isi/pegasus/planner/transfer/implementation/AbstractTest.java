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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AbstractTest {

    @Test
    public void testAbstractIsAbstractAndImplementsImplementation() {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
        assertThat(Abstract.class.getInterfaces().length, is(1));
        assertThat(
                Abstract.class.getInterfaces()[0],
                is(edu.isi.pegasus.planner.transfer.Implementation.class));
    }

    @Test
    public void testConstants() {
        assertThat(Abstract.CHANGE_XBIT_TRANSFORMATION, is("chmod"));
        assertThat(Abstract.XBIT_EXECUTABLE_BASENAME, is("chmod"));
        assertThat(Abstract.XBIT_TRANSFORMATION_NS, is("system"));
        assertThat(Abstract.XBIT_TRANSFORMATION_VERSION, nullValue());
        assertThat(Abstract.XBIT_DERIVATION_NS, is("system"));
        assertThat(Abstract.XBIT_DERIVATION_VERSION, nullValue());
        assertThat(Abstract.SET_XBIT_PREFIX, is("chmod_"));
        assertThat(Abstract.NOOP_PREFIX, is("noop_"));
    }

    @Test
    public void testConstructorAndDeclaredFieldTypes() throws Exception {
        Constructor<Abstract> constructor = Abstract.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(Modifier.isPublic(constructor.getModifiers()), is(true));

        Field propsField = Abstract.class.getDeclaredField("mProps");
        assertThat(
                propsField.getType().getName(),
                is("edu.isi.pegasus.planner.common.PegasusProperties"));

        Field siteStoreField = Abstract.class.getDeclaredField("mSiteStore");
        assertThat(
                siteStoreField.getType().getName(),
                is("edu.isi.pegasus.planner.catalog.site.classes.SiteStore"));

        Field tcHandleField = Abstract.class.getDeclaredField("mTCHandle");
        assertThat(
                tcHandleField.getType().getName(),
                is("edu.isi.pegasus.planner.catalog.TransformationCatalog"));

        Field refinerField = Abstract.class.getDeclaredField("mRefiner");
        assertThat(
                refinerField.getType().getName(), is("edu.isi.pegasus.planner.transfer.Refiner"));

        Field loggerField = Abstract.class.getDeclaredField("mLogger");
        assertThat(
                loggerField.getType().getName(), is("edu.isi.pegasus.common.logging.LogManager"));

        Field disabledChmodSitesField = Abstract.class.getDeclaredField("mDisabledChmodSites");
        assertThat(disabledChmodSitesField.getType(), is(java.util.Set.class));

        Field submitDirFactoryField = Abstract.class.getDeclaredField("mSubmitDirFactory");
        assertThat(
                submitDirFactoryField.getType().getName(),
                is("edu.isi.pegasus.planner.mapper.SubmitMapper"));
    }

    @Test
    public void testSelectedMethodSignatures() throws Exception {
        Method applyPriority =
                Abstract.class.getDeclaredMethod(
                        "applyPriority", edu.isi.pegasus.planner.classes.TransferJob.class);
        assertThat(applyPriority.getReturnType(), is(void.class));
        assertThat(Modifier.isPublic(applyPriority.getModifiers()), is(true));

        Method setRefiner =
                Abstract.class.getDeclaredMethod(
                        "setRefiner", edu.isi.pegasus.planner.transfer.Refiner.class);
        assertThat(setRefiner.getReturnType(), is(void.class));

        Method addSetXBitJobs =
                Abstract.class.getDeclaredMethod(
                        "addSetXBitJobs", Job.class, String.class, Collection.class, int.class);
        assertThat(addSetXBitJobs.getReturnType(), is(boolean.class));

        Method addSetXBitJobsWithIndex =
                Abstract.class.getDeclaredMethod(
                        "addSetXBitJobs",
                        Job.class,
                        String.class,
                        Collection.class,
                        int.class,
                        int.class);
        assertThat(addSetXBitJobsWithIndex.getReturnType(), is(boolean.class));

        Method getSetXBitJobName =
                Abstract.class.getDeclaredMethod("getSetXBitJobName", String.class, int.class);
        assertThat(getSetXBitJobName.getReturnType(), is(String.class));

        Method getNOOPJobName =
                Abstract.class.getDeclaredMethod("getNOOPJobName", String.class, int.class);
        assertThat(getNOOPJobName.getReturnType(), is(String.class));
    }
}
