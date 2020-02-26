/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.provenance.pasoa.pps;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.provenance.pasoa.PPS;

/**
 * The factory for instantiating an XMLProducer.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PPSFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME =
            "edu.isi.pegasus.planner.provenance.pasoa.pps";

    /** The default PPS implementation to be used. */
    public static final String DEFAULT_PPS_PROVIDER = "Empty";

    /** The default Pasoa PPS implementation to be used. */
    public static final String PASOA_PPS_PROVIDER = "Pasoa";

    /** The singleton instance of the PPS implementation that is returned. */
    private static PPS mInstance = null;

    /**
     * Loads the appropriate PPS implementation on the basis of the property set in the properties.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @return the instance of the appropriate XML Producer.
     * @throws PPSFactoryException that nests any error that might occur during the instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static PPS loadPPS(PegasusProperties properties) throws PPSFactoryException {

        // sanity check
        if (properties == null) {
            throw new PPSFactoryException("No properties passed to factory ");
        }

        // check for singleton
        if (mInstance != null) {
            return mInstance;
        }

        String className = properties.getRefinementProvenanceStore();
        if (className == null) {
            className = DEFAULT_PPS_PROVIDER;
        } else if (className.equalsIgnoreCase("pasoa")) {
            className = PASOA_PPS_PROVIDER;
        }

        PPS pps = null;
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
            pps = (PPS) dl.instantiate(new Object[0]);
        } catch (Exception e) {
            throw new PPSFactoryException(" Unable to instantiate PPS ", className, e);
        }
        mInstance = pps;
        return pps;
    }
}
