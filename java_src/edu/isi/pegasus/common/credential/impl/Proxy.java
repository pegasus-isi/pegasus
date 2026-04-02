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
import org.globus.common.CoGProperties;

/**
 * A convenice class that allows us to determine the path to the user proxy.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Proxy extends Abstract implements CredentialHandler {

    /** The name of the environment variable that specifies the path to the proxy. */
    public static final String X509_USER_PROXY_KEY = "X509_USER_PROXY";

    private static final String X509_USER_PROXY_PEGASUS_PROFILE_KEY =
            X509_USER_PROXY_KEY.toLowerCase(); // has to be lowercased
    /** The description. */
    private static final String DESCRIPTION = "X509 Proxy Handler";

    /** The local path to the user proxy */
    private String mLocalProxyPath;

    /** The default constructor. */
    public Proxy() {
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
        mLocalProxyPath = this.getLocalPath();
    }

    /**
     * Returns the path to user proxy. The order of preference is as follows
     *
     * <p>- If a X509_USER_PROXY is specified as a Pegasus Profile in the site catalog - Else the
     * path on the local site
     *
     * @param site the site catalog entry object.
     * @return the path to user proxy.
     */
    public String getPath(String site) {
        SiteCatalogEntry siteEntry = mSiteStore.lookup(site);

        // check if one is specified in site catalog entry
        String proxy =
                (siteEntry == null)
                        ? null
                        : (String)
                                siteEntry
                                        .getProfiles()
                                        .get(Profiles.NAMESPACES.pegasus)
                                        .get(Proxy.X509_USER_PROXY_KEY.toLowerCase());

        return (proxy == null)
                ?
                // PM-731 return the path on the local site
                this.mLocalProxyPath
                : proxy;
    }

    /**
     * Returns the path to user proxy on the local site. The order of preference is as follows
     *
     * <p>- If a proxy is specified in the site catalog entry as a Pegasus Profile that is used,
     * else the corresponding env profile for backward support - Else X509_USER_PROXY as Pegasus
     * Profile specified in the properties, else the corresponding env profile for backward support
     * - Else the one pointed to by the environment variable X509_USER_PROXY - Else the default path
     * to the proxy in /tmp is created as determined by CoGProperties.getDefault().getProxyFile()
     *
     * @param site the site catalog entry object.
     * @return the path to user proxy.
     */
    public String getLocalPath() {
        SiteCatalogEntry siteEntry = mSiteStore.lookup("local");

        // check if corresponding Pegasus Profile is specified in site catalog entry
        String proxy =
                (siteEntry == null)
                        ? null
                        : (String)
                                siteEntry
                                        .getProfiles()
                                        .get(Profiles.NAMESPACES.pegasus)
                                        .get(Proxy.X509_USER_PROXY_PEGASUS_PROFILE_KEY);
        if (proxy == null && siteEntry != null) {
            // try to check for an env profile in the site entry
            proxy =
                    (String)
                            siteEntry
                                    .getProfiles()
                                    .get(Profiles.NAMESPACES.env)
                                    .get(Proxy.X509_USER_PROXY_KEY);
        }

        // try from properties file
        if (proxy == null) {
            // load the pegasus profile from property file
            Namespace profiles = mProps.getProfiles(Profiles.NAMESPACES.pegasus);
            proxy = (String) profiles.get(Proxy.X509_USER_PROXY_PEGASUS_PROFILE_KEY);
        }
        if (proxy == null) {
            // load from property file
            Namespace env = mProps.getProfiles(Profiles.NAMESPACES.env);
            proxy = (String) env.get(Proxy.X509_USER_PROXY_KEY);
        }

        if (proxy == null) {
            // check if X509_USER_PROXY is specified in the environment
            Map<String, String> envs = System.getenv();
            if (envs.containsKey(Proxy.X509_USER_PROXY_KEY)) {
                proxy = envs.get(Proxy.X509_USER_PROXY_KEY);
            }
        }

        if (proxy == null) {
            // construct default path to user proxy in /tmp
            proxy = CoGProperties.getDefault().getProxyFile();
        }

        return proxy;
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
        return Proxy.X509_USER_PROXY_KEY;
    }

    /**
     * Returns the name of the environment variable that needs to be set for the job associated with
     * the credential.
     *
     * @return the name of the environment variable.
     */
    public String getEnvironmentVariable(String site) {
        return Proxy.X509_USER_PROXY_KEY + "_" + this.getSiteNameForEnvironmentKey(site);
    }

    /**
     * Returns the description for the implementing handler
     *
     * @return description
     */
    public String getDescription() {
        return Proxy.DESCRIPTION;
    }
}
