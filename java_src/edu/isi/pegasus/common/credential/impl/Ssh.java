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
package edu.isi.pegasus.common.credential.impl;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Namespace;
import java.io.File;
import java.util.Map;

/**
 * A convenience class that allows us to determine the path to the user ssh private key file.
 *
 * @author Mats Rynge
 * @author Karan Vahi
 * @version $Revision: 4817 $
 */
public class Ssh extends Abstract implements CredentialHandler {

    /** The name of the environment variable that specifies the path to the SSH key. */
    public static final String SSH_PRIVATE_KEY_VARIABLE = "SSH_PRIVATE_KEY";

    private static final String SSH_PRIVATE_KEY_PEGASUS_PROFILE_KEY =
            SSH_PRIVATE_KEY_VARIABLE.toLowerCase(); // has to be lowercased

    /** The description */
    private static final String DESCRIPTION = "SSH private key Credential Handler";

    /** The local path to the credential */
    private String mLocalCredentialPath;

    /** The default constructor. */
    public Ssh() {
        super();
    }

    /**
     * Initializes the credential implementation. Implementations require access to the logger,
     * properties and the SiteCatalog Store.
     *
     * @param bag the bag of Pegasus objects.
     */
    public void initialize(PegasusBag bag) {
        super.initialize(bag);
        mLocalCredentialPath = this.getLocalPath();
    }

    /**
     * Returns the path to SSH_PRIVATE_KEY . The order of preference is as follows
     *
     * <p>- If a SSH_PRIVATE_KEY is specified as a Pegasus Profile in the site catalog - Else the
     * path on the local site
     *
     * @param site the site handle
     * @return the path to SSH private key for the site.
     */
    public String getPath(String site) {

        SiteCatalogEntry siteEntry = mSiteStore.lookup(site);
        // check if one is specified in site catalog entry
        String path =
                (siteEntry == null)
                        ? null
                        : (String)
                                siteEntry
                                        .getProfiles()
                                        .get(Profiles.NAMESPACES.pegasus)
                                        .get(
                                                Ssh.SSH_PRIVATE_KEY_VARIABLE
                                                        .toLowerCase()
                                                        .toLowerCase());

        return (path == null)
                ?
                // PM-731 return the path on the local site
                this.mLocalCredentialPath
                : path;
    }

    /**
     * Returns the path to user cred on the local site. The order of preference is as follows
     *
     * <p>- If a SSH_PRIVATE_KEY is specified in the site catalog entry as a Pegasus Profile that is
     * used, else the corresponding env profile for backward support - Else the Pegasus Profile
     * SSH_PRIVATE_KEY specified in the properties, else the corresponding env profile for backward
     * support - Else the one pointed to by the environment variable SSH_PRIVATE_KEY
     *
     * @param site the site catalog entry object.
     * @return the path to user cred.
     */
    public String getLocalPath() {
        SiteCatalogEntry siteEntry = mSiteStore.lookup("local");

        // check if corresponding Pegasus Profile is specified in site catalog entry
        String cred =
                (siteEntry == null)
                        ? null
                        : (String)
                                siteEntry
                                        .getProfiles()
                                        .get(Profiles.NAMESPACES.pegasus)
                                        .get(Ssh.SSH_PRIVATE_KEY_PEGASUS_PROFILE_KEY);
        if (cred == null && siteEntry != null) {
            // try to check for an env profile in the site entry
            cred =
                    (String)
                            siteEntry
                                    .getProfiles()
                                    .get(Profiles.NAMESPACES.env)
                                    .get(Ssh.SSH_PRIVATE_KEY_VARIABLE);
        }

        // try from properites file
        if (cred == null) {
            // load the pegasus profile from property file
            Namespace profiles = mProps.getProfiles(Profiles.NAMESPACES.pegasus);
            cred = (String) profiles.get(Ssh.SSH_PRIVATE_KEY_PEGASUS_PROFILE_KEY);
        }
        if (cred == null) {
            // load the backup env profile from property file
            Namespace env = mProps.getProfiles(Profiles.NAMESPACES.env);
            cred = (String) env.get(Ssh.SSH_PRIVATE_KEY_VARIABLE);
        }

        // try from environment
        if (cred == null) {
            // check if X509_USER_PROXY is specified in the environment
            Map<String, String> envs = System.getenv();
            if (envs.containsKey(Ssh.SSH_PRIVATE_KEY_VARIABLE)) {
                cred = envs.get(Ssh.SSH_PRIVATE_KEY_VARIABLE);
            }
        }

        return cred;
    }

    /**
     * returns the basename of the path to the local credential
     *
     * @param site the site handle
     */
    public String getBaseName(String site) {
        File path = new File(this.getPath(site));
        return path.getName();
    }

    /**
     * Returns the env or pegasus profile key that needs to be associated for the credential.
     *
     * @return the name of the environment variable.
     */
    public String getProfileKey() {
        return Ssh.SSH_PRIVATE_KEY_VARIABLE;
    }

    /**
     * Returns the name of the environment variable that needs to be set for the job associated with
     * the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(String site) {
        return Ssh.SSH_PRIVATE_KEY_VARIABLE + "_" + this.getSiteNameForEnvironmentKey(site);
    }

    /**
     * Returns the description for the implementing handler
     *
     * @return description
     */
    public String getDescription() {
        return Ssh.DESCRIPTION;
    }
}
