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
package edu.isi.pegasus.planner.provenance.pasoa.producer;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.provenance.pasoa.XMLProducer;

/**
 * The factory for instantiating an XMLProducer.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class XMLProducerFactory {
    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME =
            "edu.isi.pegasus.planner.provenance.pasoa.producer";

    /** The default XML producer implementation to be used. */
    public static final String DEFAULT_XML_PRODUCER = "InMemory";

    /**
     * Loads the appropriate XMLProducer on the basis of the property set in the properties.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @return the instance of the appropriate XML Producer.
     * @throws XMLProducerFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static XMLProducer loadXMLProducer(PegasusProperties properties)
            throws XMLProducerFactoryException {

        // sanity check

        String className = DEFAULT_XML_PRODUCER;
        XMLProducer producer = null;
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
            producer = (XMLProducer) dl.instantiate(new Object[0]);
        } catch (Exception e) {
            throw new XMLProducerFactoryException(
                    " Unable to instantiate XMLProducer ", className, e);
        }
        return producer;
    }
}
