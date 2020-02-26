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
package edu.isi.pegasus.planner.selector.replica;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A replica selector that allows the user to specific regex expressions that can be used to rank
 * various PFN's returned from the Replica Catalog for a particular LFN. This replica selector
 * selects the highest ranked PFN i.e the replica with the lowest rank value.
 *
 * <p>The regular expressions are assigned different rank, that determine the order in which the
 * expressions are employed. The rank values for the regex can expressed in user properties using
 * the property. pegasus.selector.replica.regex.rank.[value]
 *
 * <p>The value is an integer value that denotes the rank of an expression with a rank value of 1
 * being the highest rank.
 *
 * <p>A thing to note is that before applying any regular expressions on the PFN's, the file URL's
 * that dont match the preferred site are explicitly filtered out.
 *
 * <p>In order to use the replica selector implemented by this class,
 *
 * <pre>
 *        - the property pegasus.selector.replica.selector must be set to value Regex
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Regex extends Default {

    /** A short description of the replica selector. */
    private static final String mDescription = "Regex";

    /** The property prefix for all Regex rank property. */
    private static final String PROPERTY_PREFIX = "pegasus.selector.replica.regex.rank.";

    /**
     * The highest value of rank. In terms of integers , the lower the int higher the rank with 1
     * being the highest value.
     */
    private static final int HIGHEST_RANK_VALUE = 1;

    /** The lowest rank value */
    private static final int LOWEST_RANK_VALUE = Integer.MAX_VALUE;

    /**
     * The Set of regular expressions that orders the regex expressions to use in ascending order.
     */
    private SortedSet<Rank> mSortedRegexSet;

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     */
    public Regex(PegasusProperties properties) {
        super(properties);
        mSortedRegexSet = getRegexSet(properties.matchingSubset(Regex.PROPERTY_PREFIX, false));
        mLogger.log(
                "[RegexReplicaSelector] User Provided Ranked regexes are " + mSortedRegexSet,
                LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * This orders amongst all the valid locations returned by the Replica Mechanism. The following
     * ordering mechanism is employed
     *
     * <p>- based on the ranks specified in the properties file.
     *
     * @param rl the <code>ReplicaLocation</code> object containing all the pfn's associated with
     *     that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica on the
     *     local site / submit host.
     * @return <code>ReplicaLocation</code> corresponding to the replicas selected
     */
    public ReplicaLocation selectAndOrderReplicas(
            ReplicaLocation rl, String preferredSite, boolean allowLocalFileURLs) {

        String lfn = rl.getLFN();
        ReplicaLocation result = new ReplicaLocation();
        result.setLFN(rl.getLFN());

        ReplicaCatalogEntry rce;

        Map<Rank, List<ReplicaCatalogEntry>> candidatesByRank =
                new TreeMap<Rank, List<ReplicaCatalogEntry>>();

        for (Iterator<ReplicaCatalogEntry> it = rl.pfnIterator(); it.hasNext(); ) {
            rce = (ReplicaCatalogEntry) it.next();
            String pfn = rce.getPFN();

            // check if a File URL is allowable or not
            if (removeFileURL(rce, preferredSite, allowLocalFileURLs)) {
                this.warnForFileURL(rce, preferredSite, allowLocalFileURLs);
                continue;
            }

            // apply the various matches
            boolean match = false;
            for (Iterator<Rank> regIt = mSortedRegexSet.iterator(); regIt.hasNext(); ) {
                Rank r = regIt.next();

                // System.out.println( "Applying regex " + r );
                if (r.matches(pfn)) {
                    match = true;
                    if (candidatesByRank.containsKey(r)) {
                        List<ReplicaCatalogEntry> l = candidatesByRank.get(r);
                        l.add(rce);
                    } else {
                        List<ReplicaCatalogEntry> l = new LinkedList();
                        l.add(rce);
                        candidatesByRank.put(r, l);
                    }

                    rce.addAttribute(
                            ReplicaSelector.PRIORITY_KEY, Integer.toString(r.getPriority()));
                    break;
                }
            }
            if (!match) {
                mLogger.log(
                        "[RegexReplicaSelector] PFN "
                                + pfn
                                + " did not match with any regex for LFN "
                                + lfn,
                        LogManager.WARNING_MESSAGE_LEVEL);
                continue;
            }
        }
        // should be ordered values
        for (List<ReplicaCatalogEntry> candidates : candidatesByRank.values()) {
            result.addPFN(candidates);
        }
        return result;
    }

    /**
     * This chooses a location amongst all the locations returned by the replica location service.
     * If a location is found with re attribute same as the preference pool, it is taken. Else a
     * random location is selected and returned. If more than one location for the lfn is found at
     * the preference pool, then also a random location amongst the ones at the preference pool is
     * selected.
     *
     * @param candidates the <code>ReplicaLocation</code> object containing all the pfn's associated
     *     with that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica on the
     *     local site / submit host.
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaCatalogEntry selectReplica(
            ReplicaLocation candidates, String preferredSite, boolean allowLocalFileURLs) {

        String lfn = candidates.getLFN();

        // create a shallow clone as we will be removing
        // using Iterator.remove() methods
        ReplicaLocation rl = (ReplicaLocation) candidates.clone();

        // log message
        StringBuffer sb = new StringBuffer();
        sb.append("[RegexReplicaSelector] Selecting a pfn for lfn ")
                .append(lfn)
                .append(" at site ")
                .append(preferredSite)
                .append("\n amongst ")
                .append(rl);
        mLogger.log(sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL);

        ReplicaCatalogEntry selectedRCE = null;
        Rank lowestRank = new Rank(Regex.LOWEST_RANK_VALUE, ".*");
        for (Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) it.next();
            String pfn = rce.getPFN();

            // check if a File URL is allowable or not
            if (removeFileURL(rce, preferredSite, allowLocalFileURLs)) {
                this.warnForFileURL(rce, preferredSite, allowLocalFileURLs);
                continue;
            }
            // System.out.println( "PFN is " + pfn );

            // apply the various Regex till you get the lowest rank value of 1

            int lowestRankValue = lowestRank.getRank();
            for (Iterator<Rank> regIt = mSortedRegexSet.iterator(); regIt.hasNext(); ) {
                Rank r = regIt.next();

                // System.out.println( "Applying regex " + r );
                if (r.matches(pfn)) {

                    // System.out.println( "Rank for pfn " + pfn + " is " + r.getRank() );
                    if (r.getRank() < lowestRankValue) {
                        selectedRCE = rce;
                        lowestRank = r;
                        lowestRankValue = r.getRank();

                        // check if the lowest rank is 1 , then we
                        // have a highest rank replica
                        if (lowestRankValue == Regex.HIGHEST_RANK_VALUE) {
                            break;
                        }
                    }
                }
            }
        }

        int numLocs = rl.getPFNCount();
        if (selectedRCE == null) {
            if (numLocs == 0) {
                // in all likelihood all the urls were file urls and
                // none were associated with the preference pool.
                // replica not selected
                StringBuffer error = new StringBuffer();
                error.append(
                                "[RegexReplicaSelector] Unable to select a Physical Filename (PFN) for file with logical filename (LFN) as ")
                        .append(rl.getLFN())
                        .append(" for staging to site ")
                        .append(preferredSite)
                        .append(" amongst ")
                        .append(candidates.getPFNList());
                throw new RuntimeException(error.toString());
            } else {
                // select a random location from all the matching locations
                selectedRCE = rl.getPFN(PegRandom.getInteger(numLocs - 1));
            }
        }

        return selectedRCE;
    }

    /**
     * Returns a short description of the replica selector.
     *
     * @return string corresponding to the description.
     */
    public String description() {
        return mDescription;
    }

    /**
     * Returns a sorted set containing the various Patterns pre-compiled. The order in the set
     * determines the order in which the patterns are applied on the PFN's
     *
     * @param properties with the key being an integer specifying the rank and value as regex
     *     expression to be applied.
     * @return Sorted<Rank>
     */
    private SortedSet<Rank> getRegexSet(Properties properties) {
        SortedSet<Rank> result = new TreeSet<Rank>();
        // traverse through the properties and put them
        // in a sorted set
        for (Iterator it = properties.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            result.add(new Rank(Integer.parseInt(key), properties.getProperty(key)));
        }

        // compute priorities
        Rank last = result.last();
        if (last != null) {
            // the highest rank has to be lowest priority
            int maxRank = last.getRank();
            for (Rank r : result) {
                r.computePriority(maxRank);
                System.out.println("Rank  " + r);
            }
        }

        // add default rank
        /* we shoud not.
        Rank lowestRank = new Rank( Regex.LOWEST_RANK_VALUE, ".*" );
        lowestRank.setPriority( 0 );
        result.add(lowestRank);
        */
        return result;
    }

    /**
     * A Data class that allows us to compile a regex expression and associate a rank value with it.
     */
    private static class Rank implements Comparable {

        /** The rank value. */
        private int mRank;

        /** The inverse of rank. Higher priority value means lower rank value */
        private int mPriority;

        /** The compiled regex expression */
        private Pattern mPattern;

        /**
         * The default constructor.
         *
         * @param rank The rank value.
         * @param regex The regex expression.
         */
        public Rank(int rank, String regex) {
            mRank = rank;
            mPattern = Pattern.compile(regex);
        }

        /**
         * Matches a string against the compiled regex expression
         *
         * @param input the input string to be matched
         * @return boolean indicating whether input matches or not.
         */
        public boolean matches(String input) {
            return mPattern.matcher(input).matches();
        }

        /**
         * Returns the underlying regex pattern associated with the Rank object.
         *
         * @return the regex pattern
         */
        public Pattern getRegex() {
            return mPattern;
        }

        /**
         * Returns the rank associated with it.
         *
         * @return the int value of the rank
         */
        public int getRank() {
            return mRank;
        }

        /**
         * Compares the Rank object with another rank object.
         *
         * @param o the object to be compared.
         * @return int
         */
        public int compareTo(Object o) {
            if (o instanceof Rank) {
                Rank r = (Rank) o;
                return (this.getRank() - r.getRank());
            } else {
                throw new ClassCastException("Object is not of class Regex.Rank");
            }
        }

        /** Returns the textual representation of this */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("( rank => ")
                    .append(getRank())
                    .append(" priority => ")
                    .append(getPriority())
                    .append(" expr => ")
                    .append(getRegex())
                    .append(")");
            return sb.toString();
        }

        /**
         * Computes priority based on the max rank passed
         *
         * @param maxRank
         */
        public void computePriority(int maxRank) {
            mPriority = (maxRank * 100) - (mRank * 100);
        }

        /** Returns the priority */
        public int getPriority() {
            return mPriority;
        }

        private void setPriority(int priority) {
            mPriority = priority;
        }
    }
}
