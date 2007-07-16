package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;

import org.griphyn.cPlanner.provenance.pasoa.XMLProducer;

/**
 * A first cut at a separate refiner interface. Right now it only has method
 * required for the PASOA integration.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Refiner {

    /**
     * The version of the API.
     */
    public static final String VERSION = "1.0";

    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     *
     * @return ADAG object.
     */
    public ADag getWorkflow();

    /**
     * Returns a reference to the XMLProducer, that generates the XML fragment
     * capturing the actions of the refiner. This is used for provenace
     * purposes.
     *
     * @return XMLProducer
     */
    public XMLProducer getXMLProducer();

}
