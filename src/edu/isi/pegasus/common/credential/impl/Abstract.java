/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.common.credential.impl;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * An abstract base class to be used by other CredentialHandler implementations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements CredentialHandler {

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The handle to the Site Catalog Store. */
    protected SiteStore mSiteStore;

    /** A handle to the logging object. */
    protected LogManager mLogger;

    /** The default constructor. */
    public Abstract() {}

    /**
     * Initializes the credential implementation. Implementations require access to the logger,
     * properties and the SiteCatalog Store.
     *
     * @param bag the bag of Pegasus objects.
     */
    public void initialize(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mSiteStore = bag.getHandleToSiteStore();
        mLogger = bag.getLogger();
    }

    /**
     * Returns the site name sanitized for use in an environment variable.
     *
     * @param site the site name.
     */
    public String getSiteNameForEnvironmentKey(String site) {
        return site.replaceAll("-", "_");
    }

    /**
     * Returns the path to the credential on the submit host.
     *
     * @return
     */
    public String getPath() {
        return this.getPath("local");
    }
}
