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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the EstimatorFactory. */
public class EstimatorFactoryTest {

    public static final class TrackingEstimator implements Estimator {
        static boolean sInitializeCalled;
        static ADag sDag;
        static PegasusBag sBag;

        static void reset() {
            sInitializeCalled = false;
            sDag = null;
            sBag = null;
        }

        @Override
        public void initialize(ADag dag, PegasusBag bag) {
            sInitializeCalled = true;
            sDag = dag;
            sBag = bag;
        }

        @Override
        public String getRuntime(Job job) {
            return null;
        }

        @Override
        public String getMemory(Job job) {
            return null;
        }

        @Override
        public Map<String, String> getAllEstimates(Job job) {
            return new HashMap<String, String>();
        }
    }

    @Test
    public void testDefaultPackageName() {
        assertThat(EstimatorFactory.DEFAULT_PACKAGE_NAME, is("edu.isi.pegasus.planner.estimate"));
    }

    @Test
    public void testDefaultEstimatorClass() {
        assertThat(EstimatorFactory.DEFAULT_ESTIMATOR_CLASS, is("Default"));
    }

    @Test
    public void testLoadDefaultEstimator() throws EstimatorFactoryException {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        Estimator estimator = EstimatorFactory.loadEstimator(null, bag);
        assertThat(estimator, notNullValue());
        assertThat(estimator, instanceOf(Default.class));
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
        assertThat(estimator, notNullValue());
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

    @Test
    public void testLoadEstimatorWithFullyQualifiedClassInvokesInitialize() throws Exception {
        TrackingEstimator.reset();
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ADag dag = new ADag();
        props.setProperty(
                "pegasus.estimator",
                "edu.isi.pegasus.planner.estimate.EstimatorFactoryTest$TrackingEstimator");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        Estimator estimator = EstimatorFactory.loadEstimator(dag, bag);

        assertThat(estimator, instanceOf(TrackingEstimator.class));
        assertThat(TrackingEstimator.sInitializeCalled, is(true));
        assertThat(TrackingEstimator.sDag, sameInstance(dag));
        assertThat(TrackingEstimator.sBag, sameInstance(bag));
    }

    @Test
    public void testLoadInvalidEstimatorStoresExpandedClassnameInException() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.estimator", "NonExistentEstimator");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        EstimatorFactoryException exception =
                assertThrows(
                        EstimatorFactoryException.class,
                        () -> EstimatorFactory.loadEstimator(null, bag),
                        "Loading a bad short name should throw");

        assertThat(
                exception.getClassname(),
                is("edu.isi.pegasus.planner.estimate.NonExistentEstimator"));
    }

    @Test
    public void testLoadDefaultEstimatorWhenPropertyUnsetStillReturnsDefaultImplementation()
            throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ADag dag = new ADag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        Estimator estimator = EstimatorFactory.loadEstimator(dag, bag);

        assertThat(estimator, instanceOf(Default.class));
    }
}
