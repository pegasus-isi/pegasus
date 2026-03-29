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
package edu.isi.pegasus.planner.estimate;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import org.junit.jupiter.api.Test;

/** Tests for the EstimatorFactory. */
public class EstimatorFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.estimate",
                EstimatorFactory.DEFAULT_PACKAGE_NAME,
                "Default package name should match");
    }

    @Test
    public void testDefaultEstimatorClass() {
        assertEquals(
                "Default",
                EstimatorFactory.DEFAULT_ESTIMATOR_CLASS,
                "Default estimator class should be 'Default'");
    }

    @Test
    public void testLoadDefaultEstimator() throws EstimatorFactoryException {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        Estimator estimator = EstimatorFactory.loadEstimator(null, bag);
        assertNotNull(estimator, "Loaded estimator should not be null");
        assertInstanceOf(Default.class, estimator, "Should load Default estimator by default");
    }

    @Test
    public void testLoadEstimatorWithNullBagThrows() {
        assertThrows(
                EstimatorFactoryException.class,
                () -> EstimatorFactory.loadEstimator(null, null),
                "Loading with null bag should throw EstimatorFactoryException");
    }

    @Test
    public void testLoadEstimatorWithNullPropertiesThrows() {
        PegasusBag bag = new PegasusBag();
        // bag without properties set
        assertThrows(
                EstimatorFactoryException.class,
                () -> EstimatorFactory.loadEstimator(null, bag),
                "Loading with null properties in bag should throw EstimatorFactoryException");
    }

    @Test
    public void testLoadEstimatorWithCustomProperty() throws EstimatorFactoryException {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.estimator", "Default");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        Estimator estimator = EstimatorFactory.loadEstimator(null, bag);
        assertNotNull(estimator, "Should load estimator specified in properties");
    }

    @Test
    public void testLoadInvalidEstimatorThrows() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.estimator", "NonExistentEstimator");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        assertThrows(
                EstimatorFactoryException.class,
                () -> EstimatorFactory.loadEstimator(null, bag),
                "Loading non-existent estimator should throw");
    }
}
