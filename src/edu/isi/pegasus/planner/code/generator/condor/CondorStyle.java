/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;


/**
 * An interface to allow us to apply different execution styles to a job
 * via Condor DAGMAN.
 *
 * Some of the common styles supported are
 *       - CondorG
 *       - Condor
 *       - Condor GlideIn
 *
 * @version $Revision$
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
     * @param siteStore   the handle to the SiteCatalog Store being used.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void initialize( PegasusProperties properties,
                            SiteStore siteStore ) throws CondorStyleException;



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
