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
package edu.isi.pegasus.planner.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ImplementationTest {

    @Test
    public void testImplementationIsInterfaceAndConstants() {
        assertThat(Implementation.class.isInterface(), is(true));
        assertThat(Implementation.VERSION, equalTo("1.6"));
        assertThat(Implementation.TRANSFER_UNIVERSE, equalTo("transfer"));
    }

    @Test
    public void testSelectedMethodReturnTypes() throws Exception {
        Method createTransferJob =
                Implementation.class.getDeclaredMethod(
                        "createTransferJob",
                        Job.class,
                        String.class,
                        Collection.class,
                        Collection.class,
                        String.class,
                        int.class);
        Method createSetXBitJob =
                Implementation.class.getDeclaredMethod(
                        "createSetXBitJob", Job.class, Collection.class, int.class, int.class);
        Method getTransformationCatalogEntry =
                Implementation.class.getDeclaredMethod(
                        "getTransformationCatalogEntry", String.class, int.class);
        Method getDescription = Implementation.class.getDeclaredMethod("getDescription");

        assertThat(createTransferJob.getReturnType(), equalTo(TransferJob.class));
        assertThat(createSetXBitJob.getReturnType(), equalTo(Job.class));
        assertThat(
                getTransformationCatalogEntry.getReturnType(),
                equalTo(TransformationCatalogEntry.class));
        assertThat(getDescription.getReturnType(), equalTo(String.class));
    }

    @Test
    public void testMethodModifiersAndDeclaredMethodCount() throws Exception {
        Method setRefiner = Implementation.class.getDeclaredMethod("setRefiner", Refiner.class);
        Method doesPreserveXBit = Implementation.class.getDeclaredMethod("doesPreserveXBit");
        Method addSetXBitJobs =
                Implementation.class.getDeclaredMethod(
                        "addSetXBitJobs",
                        Job.class,
                        String.class,
                        Collection.class,
                        int.class,
                        int.class);
        Method getSetXBitJobName =
                Implementation.class.getDeclaredMethod(
                        "getSetXBitJobName", String.class, int.class);
        Method applyPriority =
                Implementation.class.getDeclaredMethod("applyPriority", TransferJob.class);

        assertThat(Modifier.isPublic(setRefiner.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(setRefiner.getModifiers()), is(true));
        assertThat(doesPreserveXBit.getReturnType(), equalTo(boolean.class));
        assertThat(addSetXBitJobs.getReturnType(), equalTo(boolean.class));
        assertThat(getSetXBitJobName.getReturnType(), equalTo(String.class));
        assertThat(applyPriority.getReturnType(), equalTo(void.class));
        assertThat(Implementation.class.getDeclaredMethods().length, equalTo(10));
    }
}
