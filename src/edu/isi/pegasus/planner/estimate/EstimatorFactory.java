/**
 * Copyright 2007-2015 University Of Southern California
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

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * Factory class to load the estimator.
 *
 * @author Karan Vahi
 */
public class EstimatorFactory {
    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.estimate";

    /** The name of the class implementing default estimation strategy. */
    public static final String DEFAULT_ESTIMATOR_CLASS = "Default";

    /**
     * Loads the appropriate estimator.
     *
     * @param dag the workflow being planned for.
     * @param bag the bag of initialization objects.
     * @return the instance of the appropriate Estimator.
     * @throws EstimatorFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Estimator loadEstimator(ADag dag, PegasusBag bag)
            throws EstimatorFactoryException {

        if (bag == null) {
            throw new EstimatorFactoryException("Invalid instantiation with a null PegasusBag");
        }
        PegasusProperties props = bag.getPegasusProperties();
        if (props == null) {
            throw new EstimatorFactoryException(
                    "Invalid instantiation with a null Pegasus Properties");
        }

        String implementor = props.getEstimator();
        if (implementor == null) {
            implementor = EstimatorFactory.DEFAULT_ESTIMATOR_CLASS;
        }

        // now load the estimator
        Estimator estimator = null;
        String className = implementor;
        try {

            // prepend the package name if required
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            estimator = (Estimator) dl.instantiate(new Object[0]);
            estimator.initialize(dag, bag);
        } catch (Exception e) {
            throw new EstimatorFactoryException(" Unable to instantiate estimator ", className, e);
        }
        return estimator;
    }
}
