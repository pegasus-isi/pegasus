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
package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * A common class, that builds up the state from the properties to determine whether a user wants
 * certain type of transfer jobs for particular site to run remotely. This allows a user to override
 * the default behavior of how Pegasus decides whether a transfer job runs locally ( on the submit
 * host) or remotely.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RemoteTransfer {

    /** The constant to apply to all sites. */
    public static final String ALL_SITES = "*";

    /** The property name to get the sites for which all transfers need to be executed remotely. */
    public static final String ALL_TRANSFERS_REMOTE_PROPERTY = "pegasus.transfer.*.remote.sites";

    /**
     * The property name to get the sites for which stage-in transfers need to be executed remotely.
     */
    public static final String STAGE_IN_TRANSFERS_REMOTE_PROPERTY =
            "pegasus.transfer.stagein.remote.sites";

    /**
     * The property name to get the sites for which inter site transfers need to be executed
     * remotely.
     */
    public static final String INTER_TRANSFERS_REMOTE_PROPERTY =
            "pegasus.transfer.inter.remote.sites";

    /**
     * The property name to get the sites for which stage-out transfers need to be executed
     * remotely.
     */
    public static final String STAGE_OUT_TRANSFERS_REMOTE_PROPERTY =
            "pegasus.transfer.stageout.remote.sites";

    /** An internal table that maps remote transfer type to the corresponding property. */
    private static Map mPropertyTable;

    /** The handle to the properties object holding the properties relevant to Pegasus. */
    private PegasusProperties mProps;

    /** The handle to the logging object. */
    private LogManager mLogger;

    /** The map indexed by site name, that contains the state for all the sites. */
    private Map mStateMap;

    /**
     * Singleton access to the type table Contains the mapping of a property to the third party
     * transfer type
     *
     * @return map
     */
    private static Map propertyTable() {
        // singleton access
        if (mPropertyTable == null) {
            mPropertyTable = new TreeMap();
            mPropertyTable.put(
                    new Integer(TransferState.STAGE_IN_REMOTE_TYPE),
                    STAGE_IN_TRANSFERS_REMOTE_PROPERTY);
            mPropertyTable.put(
                    new Integer(TransferState.INTER_REMOTE_TYPE), INTER_TRANSFERS_REMOTE_PROPERTY);
            mPropertyTable.put(
                    new Integer(TransferState.STAGE_OUT_REMOTE_TYPE),
                    STAGE_OUT_TRANSFERS_REMOTE_PROPERTY);
            mPropertyTable.put(
                    new Integer(TransferState.ALL_REMOTE_TYPE), ALL_TRANSFERS_REMOTE_PROPERTY);
        }
        return mPropertyTable;
    }

    /** The default constructor. */
    public RemoteTransfer() {
        mProps = PegasusProperties.getInstance();
        mLogger = LogManagerFactory.loadSingletonInstance();
        mStateMap = new TreeMap();
    }

    /**
     * The overloaded constructor.
     *
     * @param properties handle to the properties required.
     */
    public RemoteTransfer(PegasusProperties properties) {
        mProps = properties;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
        mStateMap = new TreeMap();
    }

    /**
     * Builds up the remote transfers state for all the sites. This reflects what is set in the
     * properties file.
     */
    public void buildState() {
        String site;
        Set sites;
        // build for stagein transfers
        buildState(TransferState.STAGE_IN_REMOTE_TYPE);

        // build for inter site transfers
        buildState(TransferState.INTER_REMOTE_TYPE);

        // build for stage out transfers
        buildState(TransferState.STAGE_OUT_REMOTE_TYPE);

        // build for all transfers
        buildState(TransferState.ALL_REMOTE_TYPE);

        // put the all sites (site = *) entry
        TransferState allState;
        if (containsKey(ALL_SITES)) {
            allState = get(ALL_SITES);
        } else {
            allState = new TransferState();
            put(ALL_SITES, allState);
        }
        if (allState.getState() != 0x0) {
            // apply the state to all sites
            for (Iterator it = mStateMap.values().iterator(); it.hasNext(); ) {
                TransferState state = (TransferState) it.next();
                state.set(allState.getState());
            }
        }
    }

    /**
     * Adds to the existing state table, state information for a particular type of transfers.
     *
     * @param type the type of transfer.
     */
    private void buildState(int type) {
        String property = (String) propertyTable().get(new Integer(type));
        Set sites =
                getThirdPartySites(
                        (type > TransferState.ALL_REMOTE_TYPE)
                                ? mProps.getThirdPartySitesRemote(property)
                                : mProps.getThirdPartySites(property));
        String site;
        for (Iterator it = sites.iterator(); it.hasNext(); ) {
            site = (String) it.next();
            TransferState state = containsKey(site) ? get(site) : new TransferState();
            state.set(type);
            put(site, state);
        }
    }

    /**
     * Returns a boolean indicating whether to execute stage-in transfers on remote site or not.
     *
     * @return boolean
     */
    public boolean stageInOnRemoteSite(String site) {
        return containsKey(site)
                ? get(site).get(TransferState.STAGE_IN_REMOTE_TYPE)
                :
                // return the value for all sites
                get(ALL_SITES).get(TransferState.STAGE_IN_REMOTE_TYPE);
    }

    /**
     * Returns a boolean indicating whether to execute inter site transfers on remote site or not.
     *
     * @return boolean
     */
    public boolean interOnRemoteSite(String site) {
        return containsKey(site)
                ? get(site).get(TransferState.INTER_REMOTE_TYPE)
                :
                // return the value for all sites
                get(ALL_SITES).get(TransferState.INTER_REMOTE_TYPE);
    }

    /**
     * Returns a boolean indicating whether to execute stage-out transfers on remote site or not.
     *
     * @return boolean
     */
    public boolean stageOutOnRemoteSite(String site) {
        return containsKey(site)
                ? get(site).get(TransferState.STAGE_OUT_REMOTE_TYPE)
                :
                // return the value for all sites
                get(ALL_SITES).get(TransferState.STAGE_OUT_REMOTE_TYPE);
    }

    /** Prints out the third party state for the various sites. */
    public void print() {
        StringBuffer sb = new StringBuffer();
        TransferState allSitesState = null;
        Object key;
        sb.append("Site |  SI_REMOTE, IN_REMOTE , SO_REMOTE");
        for (Iterator it = mStateMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            key = entry.getKey();
            if (key.equals(ALL_SITES)) {
                // store value for printing in the end
                allSitesState = (TransferState) entry.getValue();
            } else {
                sb.append('\n').append(key).append(" | ").append(entry.getValue());
            }
        }
        if (allSitesState != null) {
            sb.append('\n').append(ALL_SITES).append("  ").append(" | ").append(allSitesState);
        }
        System.out.println(sb.toString());
    }

    /**
     * Returns whether there is an entry for a particular site or not.
     *
     * @param site the site handle for a site.
     * @return boolean
     */
    private boolean containsKey(String site) {
        return mStateMap.containsKey(site);
    }

    /**
     * Inserts an entry in to the State Map, that maintains state of various sites.
     *
     * @param site the site handle for a site.
     * @param state the thirdparty state for the site.
     */
    private void put(String site, TransferState state) {
        mStateMap.put(site, state);
    }

    /**
     * Returns the TPT state for a particular site.
     *
     * @param site the site handle for the site.
     * @return state the third party state for the site if there is an entry, else null.
     */
    private TransferState get(String site) {
        Object state = mStateMap.get(site);
        return (state == null) ? null : (TransferState) state;
    }

    /**
     * Returns a set of third party sites. An empty set is returned if value is null.
     *
     * @param value the value in the properties file.
     * @return Set containing the names of the pools.
     */
    private Set getThirdPartySites(String value) {
        HashSet set = new HashSet();
        String site;
        if (value == null) {
            return set;
        }

        for (StringTokenizer st = new StringTokenizer(value, ","); st.hasMoreTokens(); ) {
            site = (String) st.nextToken();
            /*
            mLogger.log(site + " is a third party enabled site " +
                        "for " + desc + " transfers",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            */
            set.add(site);
        }
        return set;
    }

    /**
     * An inner class that holds the state for a particular site,as to whether to execute transfers
     * remotely or not.
     */
    private static class TransferState {

        /** The constant to denote that a stage-in transfer is to be exectuted remotely */
        public static final int STAGE_IN_REMOTE_TYPE = 0x1; // 000001

        /** The constant to denote that an inter site transfer is to be exectuted remotely */
        public static final int INTER_REMOTE_TYPE = 0x2; // 000010

        /** The constant to denote that a stage-out transfer is to be exectuted remotely */
        public static final int STAGE_OUT_REMOTE_TYPE = 0x4; // 000100

        /** The constant to denote that all transfers are to be exectuted remotely */
        public static final int ALL_REMOTE_TYPE = 0x7; // 000111

        /** Stores the state as an integer. */
        private int mState;

        /** The default constructor. */
        public TransferState() {
            mState = 0x0;
        }

        /**
         * Returns the state.
         *
         * @return the state as an int
         */
        public int getState() {
            return mState;
        }

        /**
         * Sets a type of transfer to be third party.
         *
         * @param type the type of transfer to be set TPT
         */
        public void set(int type) {
            // no type checking for time being
            mState = mState | type;
        }

        /**
         * Returns a boolean indicating whether the attribute passed is set in the transfer state or
         * not. The attribute types are as constants in this class.
         *
         * @param type the attribute type.
         */
        public boolean get(int type) {
            return ((mState & type) == type);
        }

        /**
         * Returns a textual description of the state as (stageinRemote,interRemote,stageoutRemote).
         *
         * @return the textual description.
         */
        public String toString() {
            StringBuffer sb = new StringBuffer(36);
            sb.append('(')
                    .append(this.get(TransferState.STAGE_IN_REMOTE_TYPE))
                    .append("  ")
                    .append(',')
                    .append(this.get(TransferState.INTER_REMOTE_TYPE))
                    .append("  ")
                    .append(',')
                    .append(this.get(TransferState.STAGE_OUT_REMOTE_TYPE))
                    .append(')');
            return sb.toString();
        }
    }
}
