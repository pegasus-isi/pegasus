/**
 * Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.mapper.staging;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import java.io.File;
import java.util.Properties;

/** @author Karan Vahi */
public abstract class Abstract implements StagingMapper {

    /** Handle to the logger */
    protected LogManager mLogger;

    protected SiteStore mSiteStore;

    public Abstract() {}

    /**
     * Initializes the submit mapper
     *
     * @param bag the bag of Pegasus objects
     * @param properties properties that can be used to control the behavior of the mapper
     */
    public void initialize(PegasusBag bag, Properties properties) {
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
    }

    /**
     * Maps a LFN to a location on the filesystem of a site and returns a single externally
     * accessible URL corresponding to that location.
     *
     * @param job
     * @param addOn
     * @param site the staging site
     * @param operation whether we want a GET or a PUT URL
     * @param lfn the lfn
     * @return the URL to file that was mapped
     * @throws MapperException if unable to construct URL for any reason
     */
    public String map(
            Job job, File addOn, SiteCatalogEntry site, FileServer.OPERATION operation, String lfn)
            throws MapperException {
        StringBuffer url = new StringBuffer();

        FileServer getServer = site.selectHeadNodeScratchSharedFileServer(operation);
        String siteHandle = site.getSiteHandle();
        if (getServer == null) {
            this.complainForScratchFileServer(job, operation, siteHandle);
        }

        url.append(getServer.getURLPrefix())
                .append(mSiteStore.getExternalWorkDirectory(getServer, siteHandle));

        // check if we already have placed this file on the staging site
        // use that addOn then.
        url.append(File.separatorChar).append(addOn);

        if (lfn != null) {
            url.append(File.separatorChar).append(lfn);
        }

        return url.toString();
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param job the job
     * @param operation the operation
     * @param site the site
     */
    protected void complainForScratchFileServer(
            Job job, FileServer.OPERATION operation, String site) {
        this.complainForScratchFileServer(job.getID(), operation, site);
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param jobname the name of the job
     * @param operation the file server operation
     * @param site the site
     */
    protected void complainForScratchFileServer(
            String jobname, FileServer.OPERATION operation, String site) {
        StringBuffer error = new StringBuffer();
        error.append("[").append(this.description()).append("] ");
        if (jobname != null) {
            error.append("For job (").append(jobname).append(").");
        }
        error.append(" File Server not specified for shared-scratch filesystem for site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }
}
