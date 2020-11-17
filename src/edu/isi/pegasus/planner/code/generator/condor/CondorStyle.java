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
package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * An interface to allow us to apply different execution styles to a job via Condor DAGMAN.
 *
 * <p>Some of the common styles supported are - CondorG - Condor - Condor GlideIn
 *
 * @version $Revision$
 */
public interface CondorStyle {

    /** The version number associated with this API of Code Generator. */
    public static final String VERSION = "1.4";

    /**
     * Initializes the Code Style implementation.
     *
     * @param bag the bag of initialization objects
     * @param credentialFactory the credential handler factory
     * @throws CondorStyleFactoryException that nests any error that might occur during the
     *     instantiation of the implementation.
     */
    public void initialize(PegasusBag bag, CredentialHandlerFactory credentialFactory)
            throws CondorStyleException;

    /**
     * Apply a style to a SiteCatalogEntry. This allows the style classes to add or modify the
     * existing profiles for the site so far.
     *
     * @param site the site catalog entry object
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(SiteCatalogEntry site) throws CondorStyleException;

    /**
     * Apply a style to a job. Involves changing the job object, and optionally writing out to the
     * Condor submit file.
     *
     * @param job the <code>Job</code> object containing the job.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException;

    /**
     * Apply a style to an AggregatedJob
     *
     * @param job the <code>AggregatedJob</code> object containing the job.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(AggregatedJob job) throws CondorStyleException;
}
