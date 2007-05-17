/**
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

package org.griphyn.cPlanner.code.generator.condor;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

/**
 * An interface to allow us to apply different execution styles to a job
 * via Condor DAGMAN.
 *
 * Some of the common styles supported are
 *       - CondorG
 *       - Condor
 *       - Condor GlideIn
 *
 * @version $Revision: 1.1 $
 */

public interface CondorStyle {

    /**
     * The version number associated with this API of Code Generator.
     */
    public static final String VERSION = "1.1";


    /**
     * Initializes the Condor Style implementation.
     *
     * @param properties  the <code>PegasusProperties</code> object containing all
     *                    the properties required by Pegasus.
     * @param siteCatalog a handle to the Site Catalog being used.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            PoolInfoProvider siteCatalog) throws CondorStyleException;



    /**
     * Apply a style to a job. Involves changing the job object, and optionally
     * writing out to the Condor submit file.
     *
     * @param job  the <code>SubInfo</code> object containing the job.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( SubInfo job ) throws CondorStyleException;

}