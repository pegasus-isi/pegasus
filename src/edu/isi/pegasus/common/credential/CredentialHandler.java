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
package edu.isi.pegasus.common.credential;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * The credential interface that defines the credentials that can be associated with jobs.
 *
 * @author Karan Vahi
 */
public interface CredentialHandler {

    /** The version of the API being used. */
    public static final String VERSION = "1.3";

    // type of credentials associated
    /** An enumeration of valid types of credentials that are supported. */
    public static enum TYPE {
        credentials,
        x509,
        s3,
        boto,
        googlep12,
        irods,
        ssh
    };

    /**
     * Initializes the credential implementation. Implementations require access to the logger,
     * properties and the SiteCatalog Store.
     *
     * @param bag the bag of Pegasus objects.
     */
    public void initialize(PegasusBag bag);

    /**
     * Returns the path to the credential on the submit host.
     *
     * @return
     */
    public String getPath();

    /**
     * Returns the path to the credential for a particular site handle
     *
     * @param site the site catalog entry object.
     * @return the path to the credential
     */
    public String getPath(String site);

    /**
     * Returns the env or pegasus profile key that needs to be associated for the credential.
     *
     * @return the name of the environment variable.
     */
    public String getProfileKey();

    /**
     * Returns the name of the environment variable that needs to be set for the job associated with
     * the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(String site);

    /**
     * Returns the description for the implementing handler
     *
     * @return description
     */
    public String getDescription();

    /**
     * returns the basename of the credential file name
     *
     * @param site the side to associate with.
     * @return
     */
    public String getBaseName(String site);

    /**
     * Verify a local credential accessible via path specified
     *
     * @param job the job with which the credential is associated
     * @param type the type of credential
     * @param path the path to the credential
     * @throws RuntimeException in case of being unable to verify credential.
     */
    public void verify(Job job, CredentialHandler.TYPE type, String path);
}
