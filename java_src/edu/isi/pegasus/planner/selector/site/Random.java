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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.List;

/**
 * A random site selector that maps to a job to a random pool, amongst the subset of pools where
 * that particular job can be executed.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Random extends AbstractPerJob {

    /** The default constructor. Should not be called. Call the overloaded one. */
    public Random() {}

    /**
     * Initializes the site selector.
     *
     * @param bag the bag of objects that is useful for initialization.
     */
    public void initialize(PegasusBag bag) {
        super.initialize(bag);
    }

    /**
     * Maps a job in the workflow to an execution site.
     *
     * @param job the job to be mapped.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public void mapJob(Job job, List sites) {

        List rsites =
                mTCMapper.getSiteList(
                        job.getTXNamespace(), job.getTXName(), job.getTXVersion(), sites);

        if (rsites == null || rsites.isEmpty()) {
            job.setSiteHandle(null);
        } else {
            job.setSiteHandle(selectRandomSite(rsites));
            StringBuffer message = new StringBuffer();
            message.append("[Random Selector] Mapped ")
                    .append(job.getID())
                    .append(" to ")
                    .append(job.getSiteHandle());
            mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Returns a brief description of the site selection technique being used.
     *
     * @return String
     */
    public String description() {
        String st = "Random Site Selection";
        return st;
    }

    /**
     * The random selection that selects randomly one of the records returned by the transformation
     * catalog.
     *
     * @param sites List of <code>String</code>objects.
     * @return String
     */
    private String selectRandomSite(List sites) {
        double randNo;
        int noOfRecs = sites.size();

        // means we have to choose a random location between 0 and (noOfLocs -1)
        randNo = Math.random() * noOfRecs;
        int recSelected = new Double(randNo).intValue();
        /*
        String message = "Random Site selected is " + (recSelected + 1) +
            " amongst " + noOfRecs + " possible";
        mLogger.logMessage(message, 1, false);
        */
        return (String) sites.get(recSelected);
    }
}
