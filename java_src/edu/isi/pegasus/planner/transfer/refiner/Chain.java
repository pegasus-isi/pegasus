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
package edu.isi.pegasus.planner.transfer.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This transfer refiner builds upon the Default Refiner. The defaul Refiner allows the transfer of
 * multiple files in a single condor job. However, it adds the stage in transfer nodes in parallel
 * leading to multiple invocation of the globus-url-copy at remote execution pools, while running
 * huge workflows. This refiner, tries to circumvent this problem by chaining up the stagein jobs
 * instead of scheduling in parallel. This works best only when the top level of the workflow
 * requires stage in jobs. The correct way is that the traversal needs to be done breath first in
 * the TransferEngine.java.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Chain extends Basic {

    /**
     * The default bundling factor that identifies the number of transfer jobs that are being
     * created per execution pool for the workflow.
     */
    public static final String DEFAULT_BUNDLE_FACTOR = "1";

    /** The handle to the Site Catalog. It is instantiated in this class. */
    // protected PoolInfoProvider mSCHandle;
    protected SiteStore mSiteStore;

    /**
     * The map containing the stage in bundle values indexed by the name of the site. If the bundle
     * value is not specified, then null is stored.
     */
    private Map mSIBundleMap;

    /**
     * A map indexed by execution sites. Each value is a SiteTransfer object, that contains the
     * Bundles of stagin transfer jobs.
     *
     * @see TransferChain
     */
    private Map mStageInMap;

    /** A short description of the transfer refinement. */
    public static final String DESCRIPTION =
            "Chain Mode (the stage in jobs being chained together in bundles";

    /**
     * The overloaded constructor.
     *
     * @param dag the workflow to which transfer nodes need to be added.
     * @param bag the bag of initialization objects
     */
    public Chain(ADag dag, PegasusBag bag) {
        super(dag, bag);
        // specifying initial capacity.
        // adding one to account for local pool
        mStageInMap = new HashMap(mPOptions.getExecutionSites().size() + 1);
        mSIBundleMap = new HashMap();

        // load the site catalog
        mSiteStore = bag.getHandleToSiteStore();
    }

    /**
     * Adds a new relation to the workflow. In the case when the parent is a transfer job that is
     * added, the parentNew should be set only the first time a relation is added. For subsequent
     * compute jobs that maybe dependant on this, it needs to be set to false.
     *
     * @param parent the jobname of the parent node of the edge.
     * @param child the jobname of the child node of the edge.
     * @param site the execution site where the transfer node is to be run.
     * @param parentNew the parent node being added, is the new transfer job and is being called for
     *     the first time.
     */
    public void addRelation(String parent, String child, String site, boolean parentNew) {

        addRelation(parent, child);
        //        mDAG.addNewRelation(parent,child);

        if (parentNew) {
            // a new transfer job is being added
            // figure out the correct bundle to
            // put in
            List l = null;
            if (mStageInMap.containsKey(site)) {
                // get the SiteTransfer for the site
                SiteTransfer old = (SiteTransfer) mStageInMap.get(site);
                // put the parent in the appropriate bundle
                // and get the pointer to the last element in
                // the chain before the parent is added.
                String last = old.addTransfer(parent);
                if (last != null) {
                    // the parent is now the last element in the chain
                    // continue the chain forward
                    // adding the last link in the chain
                    this.addRelation(last, parent, site, false);
                }
            } else {
                // create a new SiteTransfer for the job
                // determine the bundle for the site
                int bundle;
                if (mSIBundleMap.containsKey(site)) {
                    bundle = ((Integer) mSIBundleMap.get(site)).intValue();
                } else {
                    bundle = getSiteBundleValue(site, Pegasus.CHAIN_STAGE_IN_KEY);
                    // put the value into the map
                    mSIBundleMap.put(site, new Integer(bundle));
                }
                SiteTransfer siteTX = new SiteTransfer(site, bundle);
                siteTX.addTransfer(parent);
                mStageInMap.put(site, siteTX);
            }
        }
    }

    /**
     * Determines the bundle factor for a particular site on the basis of the key associcated with
     * the underlying transfer transformation in the transformation catalog. If none specified in
     * transformation catalog then one is picked up from the site catalog. If the key is not found
     * in the site catalog too , then the global default is returned.
     *
     * @param site the site at which the transfer job is being run.
     * @param key the bundle key whose value needs to be searched.
     * @return the bundle factor.
     * @see #DEFAULT_BUNDLE_FACTOR
     */
    public int getSiteBundleValue(String site, String key) {
        String value = this.DEFAULT_BUNDLE_FACTOR;
        // construct a sudo transfer job object
        // and populate the profiles in it.
        Job sub = new Job();
        // assimilate the profile information from the
        // site catalog into the job.
        sub.updateProfiles(mSiteStore.lookup(site).getProfiles());

        // this should be parameterised Karan Dec 20,2005
        TransformationCatalogEntry entry =
                mTXStageInImplementation.getTransformationCatalogEntry(site, Job.STAGE_IN_JOB);

        // assimilate the profile information from transformation catalog
        if (entry != null) {
            sub.updateProfiles(entry);
        }

        value = (sub.vdsNS.containsKey(key)) ? sub.vdsNS.getStringValue(key) : value;
        return Integer.parseInt(value);
    }

    /** Prints out the bundles and chains that have been constructed. */
    public void done() {
        super.done();
        // print out all the Site transfers that you have
        mLogger.log("Chains of stagein jobs per sites are ", LogManager.DEBUG_MESSAGE_LEVEL);
        for (Iterator it = mStageInMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            mLogger.log(entry.getKey() + " " + entry.getValue(), LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return this.DESCRIPTION;
    }

    /**
     * A container to manage the transfer jobs that are needed to be done on a single site. The
     * container maintains the bundles and controls the distribution of a transfer job amongst the
     * bundles in a round robin manner. Each bundle itself is actually a chain of transfer jobs.
     */
    private static class SiteTransfer {

        /**
         * The maximum number of transfer jobs that are allowed for this particular site. This
         * should correspond to the bundle factor.
         */
        private int mCapacity;

        /** The index of the bundle to which the next transfer for the site would be added to. */
        private int mNext;

        /** The site for which these transfers are grouped. */
        private String mSite;

        /**
         * The list of <code>Chain</code> object. Each bundle is actually a chain of transfer nodes.
         */
        private List mBundles;

        /** The default constructor. */
        public SiteTransfer() {
            mCapacity = 1;
            mNext = -1;
            mSite = null;
            mBundles = null;
        }

        /**
         * Convenience constructor.
         *
         * @param pool the pool name for which transfers are being grouped.
         * @param bundle the number of logical bundles that are to be created per site. it directly
         *     translates to the number of transfer jobs that can be running at a particular site
         */
        public SiteTransfer(String pool, int bundle) {
            mCapacity = bundle;
            mNext = 0;
            mSite = pool;
            mBundles = new ArrayList(bundle);
            // intialize to null
            for (int i = 0; i < bundle; i++) {
                mBundles.add(null);
            }
        }

        /**
         * Adds a file transfer to the appropriate TransferChain. The file transfers are added in a
         * round robin manner underneath.
         *
         * @param txJobName the name of the transfer job.
         * @return the last transfer job in the chain before the current job was added, null in case
         *     the job is the first in the chain
         */
        public String addTransfer(String txJobName) {
            // hmmm i could alternatively add using the
            // iterator and move iterator around.

            // we add the transfer to the chain pointed
            // by next
            Object obj = mBundles.get(mNext);
            TransferChain chain = null;
            String last = null;
            if (obj == null) {
                // on demand add a new chain to the end
                // is there a scope for gaps??
                chain = new TransferChain();
                mBundles.set(mNext, chain);
            } else {
                chain = (TransferChain) obj;
            }
            // we have the chain to which we want
            // to add the transfer job. Get the
            // current last job in the chain before
            // adding the transfer job to the chain
            last = chain.getLast();
            chain.add(txJobName);
            // update the next pointer to maintain
            // round robin status
            mNext = (mNext < (mCapacity - 1)) ? mNext + 1 : 0;
            return last;
        }

        /**
         * Returns the textual description of the object.
         *
         * @return the textual description.
         */
        public String toString() {
            StringBuffer sb = new StringBuffer(32);
            boolean first = true;
            sb.append("Site ").append(mSite);
            int num = 1;
            for (Iterator it = mBundles.iterator(); it.hasNext(); num++) {
                sb.append("\n").append(num).append(" :").append(it.next());
            }
            return sb.toString();
        }
    }

    /**
     * A shallow container class, that contains the list of the names of the transfer jobs and can
     * return the last job in the list.
     */
    private static class TransferChain {

        /** The linked list that maintians the chain of names of the transfer jobs. */
        private LinkedList mChain;

        /** The default constructor. */
        public TransferChain() {
            mChain = new LinkedList();
        }

        /**
         * Adds to the end of the chain. Allows null to be added.
         *
         * @param name the name of the transfer job.
         */
        public void add(String name) {
            mChain.addLast(name);
        }

        /**
         * Returns the last element in the chain.
         *
         * @return the last element in the chain, null if the chain is empty
         */
        public String getLast() {
            String last = null;
            try {
                last = (String) mChain.getLast();
            } catch (java.util.NoSuchElementException e) {

            }
            return last;
        }

        /**
         * Returns the textual description of the object.
         *
         * @return the textual description.
         */
        public String toString() {
            StringBuffer sb = new StringBuffer(32);
            boolean first = true;
            for (Iterator it = mChain.iterator(); it.hasNext(); ) {
                if (first) {
                    first = false;
                } else {
                    sb.append("->");
                }
                sb.append(it.next());
            }
            return sb.toString();
        }
    }
}
