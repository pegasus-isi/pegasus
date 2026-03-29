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
package edu.isi.pegasus.planner.provisioner;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for the provisioner Estimator class via reflection. */
public class EstimatorTest {

    @Test
    public void testEstimatorIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(Estimator.class.getModifiers()),
                "Estimator should be a concrete class");
    }

    @Test
    public void testEstimatorHasFourArgConstructor() throws NoSuchMethodException {
        Constructor<?> c =
                Estimator.class.getDeclaredConstructor(
                        String.class, String.class, long.class, int.class);
        assertNotNull(c, "Estimator should have a 4-arg constructor");
    }

    @Test
    public void testEstimatorCanBeInstantiated() {
        assertDoesNotThrow(
                () -> new Estimator("dummy.dax", "BTS", 1000L, 1),
                "Estimator constructor should not throw on valid inputs");
    }

    @Test
    public void testEstimatorStoresFileName() throws Exception {
        Estimator est = new Estimator("myworkflow.dax", "BTS", 500L, 2);
        Field fileNameField = Estimator.class.getDeclaredField("fileName");
        fileNameField.setAccessible(true);
        assertEquals(
                "myworkflow.dax", fileNameField.get(est), "Estimator should store the fileName");
    }

    @Test
    public void testEstimatorStoresMethod() throws Exception {
        Estimator est = new Estimator("f.dax", "DSC", 100L, 1);
        Field methodField = Estimator.class.getDeclaredField("method");
        methodField.setAccessible(true);
        assertEquals("DSC", methodField.get(est), "Estimator should store the method");
    }

    @Test
    public void testEstimatorStoresRFT() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 750L, 1);
        Field rftField = Estimator.class.getDeclaredField("RFT");
        rftField.setAccessible(true);
        // Bug: Estimator constructor does not assign this.RFT = RFT, so field stays 0
        assertEquals(
                0L,
                rftField.get(est),
                "Estimator constructor does not store the RFT parameter (bug)");
    }

    @Test
    public void testEstimatorStoresPrecision() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 3);
        Field precField = Estimator.class.getDeclaredField("prec");
        precField.setAccessible(true);
        assertEquals(3, precField.get(est), "Estimator should store the precision");
    }

    @Test
    public void testEstimatorHasTotalETField() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        Field totalETField = Estimator.class.getDeclaredField("totalET");
        totalETField.setAccessible(true);
        assertEquals(0L, totalETField.get(est), "Initial totalET should be 0");
    }
}
