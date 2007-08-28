/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.selector;



import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.partitioner.graph.Graph;

import java.util.List;

/**
 *
 * The interface for the Site Selector. Allows us to maps the workflows
 * to different sites.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gaurang Mehta
 *
 *
 * @version $Revision$
 */
public interface SiteSelector {

    /**
     * The version of the API of the Site Selector.
     */
    public static final String VERSION = "2.0";

    /**
     * The value for the pool handle, when the pool is not found.
     */
    public static final String SITE_NOT_FOUND = "NONE";


    /**
     * Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( PegasusBag bag );

    /**
     * Maps the jobs in the workflow to the various grid sites.
     * The jobs are mapped by setting the site handle for the jobs.
     *
     * @param workflow   the workflow in a Graph form.
     *
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution sites that can be used.
     */
    public void mapWorkflow( Graph workflow, List sites );

    /**
     * Maps the jobs in the workflow to the various grid sites.
     * The jobs are mapped by setting the site handle for the jobs.
     *
     * @param workflow   the workflow.
     *
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution sites that can be used.
     */
    public void mapWorkflow( ADag workflow, List sites );



    /**
     * This method returns a String describing the site selection technique
     * that is being implemented by the implementing class.
     *
     * @return a short description
     */
    public  String description();



}
