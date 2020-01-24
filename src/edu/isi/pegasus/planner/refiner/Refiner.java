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
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.provenance.pasoa.XMLProducer;

/**
 * A first cut at a separate refiner interface. Right now it only has method required for the PASOA
 * integration.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Refiner {

    /** The version of the API. */
    public static final String VERSION = "1.0";

    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     * @return ADAG object.
     */
    public ADag getWorkflow();

    /**
     * Returns a reference to the XMLProducer, that generates the XML fragment capturing the actions
     * of the refiner. This is used for provenace purposes.
     *
     * @return XMLProducer
     */
    public XMLProducer getXMLProducer();
}
