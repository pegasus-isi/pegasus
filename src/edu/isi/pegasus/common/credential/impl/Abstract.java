/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.common.credential.impl;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Set;

/**
 * An abstract base class to be used by other CredentialHandler implementations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements CredentialHandler {

    public static final String POSIX_600_PERMISSIONS_STRING = "rw-------";

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The handle to the Site Catalog Store. */
    protected SiteStore mSiteStore;

    /** A handle to the logging object. */
    protected LogManager mLogger;

    /** set of credentials that are already verified. */
    protected Set<Path> mVerifiedCredentials;

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
        mVerifiedCredentials = new HashSet();
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

    /**
     * Verify a local credential accessible via path specified
     *
     * @param job the job with which the credential is associated
     * @param type the type of credential
     * @param path the path to the credential
     * @throws RuntimeException in case of being unable to verify credential.
     */
    public void verifyCredential(Job job, CredentialHandler.TYPE type, String path) {
        Path credPath = Paths.get(path);
        // short circuit
        if (this.mVerifiedCredentials.contains(credPath)) {
            return;
        }
        StringBuilder error = new StringBuilder();
        error.append("exists and has following permissions")
                .append(" ")
                .append(Abstract.POSIX_600_PERMISSIONS_STRING)
                .append(".");
        boolean verify = Files.exists(credPath);
        if (verify) {
            Set<PosixFilePermission> perms = null;
            try {
                perms = Files.readAttributes(credPath, PosixFileAttributes.class).permissions();
                String permString = PosixFilePermissions.toString(perms);
                verify = permString.equals(Abstract.POSIX_600_PERMISSIONS_STRING);
                if (!verify) {
                    error.append("\n")
                            .append("Existing file has")
                            .append(" ")
                            .append(permString)
                            .append(" ")
                            .append("permissions.");
                }
            } catch (IOException ex) {
                verify = false;
            }
        }

        if (verify) {
            // cache this as verified credential
            this.mVerifiedCredentials.add(credPath);
        } else {
            this.complainForInvalidCredential(job, type, path, error.toString());
        }
    }

    /**
     * Complain if a particular credential is un-verified, as the moment only checks for file
     * existence
     *
     * @param job
     * @param type the type of credential
     * @param path
     * @throws RuntimeException
     */
    protected void complainForInvalidCredential(
            Job job, CredentialHandler.TYPE type, String path, String errorSuffix)
            throws RuntimeException {
        StringBuilder error = new StringBuilder();

        error.append("Unable to verify credential of type")
                .append(" ")
                .append(type)
                .append(" ")
                .append("for job")
                .append(" ")
                .append(job.getName())
                .append(".")
                .append("\n")
                .append("Please make sure the credential")
                .append(" ")
                .append(path)
                .append(" ")
                .append(errorSuffix);
        throw new RuntimeException(error.toString());
    }
}
