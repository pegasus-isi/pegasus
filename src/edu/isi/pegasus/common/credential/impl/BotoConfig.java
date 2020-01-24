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
 * A convenience class that allows us to determine the path to the user boto config file.
 *
 * @author Mats Rynge
 * @author Karan Vahi
 * @version $Revision$
 */
public class BotoConfig extends Abstract implements CredentialHandler {

    /** The name of the environment variable that specifies the path to the s3cfg file. */
    public static final String BOTO_CONFIG_FILE_VARIABLE = "BOTO_CONFIG";

    private static final String BOTO_CONFIG_PEGASUS_PROFILE_KEY =
            BotoConfig.BOTO_CONFIG_FILE_VARIABLE.toLowerCase(); // has to be lowercased

    /** The description */
    private static final String DESCRIPTION = "Boto Config File Credential Handler";

    /** The local path to the credential */
    private String mLocalCredentialPath;

    /** The default constructor. */
    public BotoConfig() {
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
     * Returns the path to the Boto config file . The order of preference is as follows
     *
     * <p>- If a BOTO_CONFIG is specified as a Pegasus Profile in the site catalog - Else the path
     * on the local site
     *
     * @param site the site handle
     * @return the path to BotoConfig.BOTO_CONFIG_FILE_VARIABLE for the site.
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
                                        .get(BotoConfig.BOTO_CONFIG_FILE_VARIABLE.toLowerCase());

        return (path == null)
                ?
                // PM-731 return the path on the local site
                mLocalCredentialPath
                : path;
    }

    /**
     * Returns the path to user cred on the local site. The order of preference is as follows
     *
     * <p>- If a BotoConfig.BOTO_CONFIG_FILE_VARIABLE is specified in the site catalog entry as a
     * Pegasus Profile that is used, else the corresponding env profile for backward support - Else
     * BotoConfig.BOTO_CONFIG_FILE_VARIABLE Pegasus Profile specified in the properties, else the
     * corresponding env profile for backward support - Else the one pointed to by the environment
     * variable BotoConfig.BOTO_CONFIG_FILE_VARIABLE
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
                                        .get(BotoConfig.BOTO_CONFIG_PEGASUS_PROFILE_KEY);
        if (cred == null && siteEntry != null) {
            // try to check for an env profile in the site entry
            cred =
                    (String)
                            siteEntry
                                    .getProfiles()
                                    .get(Profiles.NAMESPACES.env)
                                    .get(BotoConfig.BOTO_CONFIG_FILE_VARIABLE);
        }

        // try from properites file
        if (cred == null) {
            // load the pegasus profile from property file
            Namespace profiles = mProps.getProfiles(Profiles.NAMESPACES.pegasus);
            cred = (String) profiles.get(BotoConfig.BOTO_CONFIG_PEGASUS_PROFILE_KEY);
        }
        if (cred == null) {
            // load the env profile from the  property file
            Namespace env = mProps.getProfiles(Profiles.NAMESPACES.env);
            cred = (String) env.get(BotoConfig.BOTO_CONFIG_FILE_VARIABLE);
        }

        if (cred == null) {
            // check if BOTO_CONFIG is specified in the environment
            Map<String, String> envs = System.getenv();
            if (envs.containsKey(BotoConfig.BOTO_CONFIG_FILE_VARIABLE)) {
                cred = envs.get(BotoConfig.BOTO_CONFIG_FILE_VARIABLE);
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
        return BotoConfig.BOTO_CONFIG_FILE_VARIABLE;
    }

    /**
     * Returns the name of the environment variable that needs to be set for the job associated with
     * the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(String site) {
        return BotoConfig.BOTO_CONFIG_FILE_VARIABLE + "_" + this.getSiteNameForEnvironmentKey(site);
    }

    /**
     * Returns the description for the implementing handler
     *
     * @return description
     */
    public String getDescription() {
        return BotoConfig.DESCRIPTION;
    }
}
