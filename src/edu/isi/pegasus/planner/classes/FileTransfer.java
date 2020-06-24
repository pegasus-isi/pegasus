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
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a container for the storing the transfers that are required in between sites. It refers
 * to one lfn, but can contains more than one source and destination urls. All the source url's are
 * presumed to be identical. The destination urls, can in effect be used to refer to TFN's for a lfn
 * on different sites.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class FileTransfer extends PegasusFile {

    /**
     * The logical name of the asssociated VDS super node, with which the file is associated. The
     * name of the job can be of the job that generates that file(while doing intersite or
     * transferring output files to output site) or of a job for which the file is an input(getting
     * an input file from the Replica Services).
     */
    private String mJob;

    /**
     * The map containing all the source urls keyed by the site id/name. Corresponding to each site,
     * a list of url's is stored that contain the URL's for that site. All url's not associated with
     * a site, are associated with a undefined site.
     */
    private Map<String, List<ReplicaCatalogEntry>> mSourceMap;

    /**
     * The map containing all the destination urls keyed by the site id/name. Corresponding to each
     * site, a list of url's is stored that contain the URL's for that site. All url's not
     * associated with a site, are associated with a undefined site.
     */
    private Map<String, List<ReplicaCatalogEntry>> mDestMap;

    /** The registration URL for the file */
    private String mURLForRegistrationOnDestination;

    /** A priority associated with the FileTransfer */
    private int mPriority;

    /** A boolean indicating to force symlinks even if source does not exist */
    private boolean mVerifySymlinkSource;

    /** Default constructor. */
    public FileTransfer() {
        super();
        mJob = "";
        mFlags = new BitSet(NO_OF_TRANSIENT_FLAGS);
        mSourceMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        mDestMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        mPriority = 0;
        mURLForRegistrationOnDestination = null;
        mVerifySymlinkSource = true;
    }

    /**
     * The overloaded constructor.
     *
     * @param pf <code>PegasusFile</code> object containing the transiency attributes, and the
     *     logical name of the file.
     */
    public FileTransfer(PegasusFile pf) {
        this.mLogicalFile = pf.mLogicalFile;
        this.mTransferFlag = pf.mTransferFlag;
        this.mSize = pf.mSize;
        this.mFlags = pf.getFlags();
        this.mType = pf.getType();
        this.mJob = "";
        this.mSourceMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        this.mDestMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        this.mPriority = 0;
        this.mVerifySymlinkSource = true;
        this.mURLForRegistrationOnDestination = null;
        this.mMetadata = pf.getAllMetadata();
    }

    /**
     * The overloaded constructor.
     *
     * @param lfn The logical name of the file that has to be transferred.
     * @param job The name of the job with which the transfer is associated with.
     */
    public FileTransfer(String lfn, String job) {
        super(lfn);
        mJob = job;
        mSourceMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        mDestMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        this.mPriority = 0;
        this.mURLForRegistrationOnDestination = null;
        this.mVerifySymlinkSource = true;
    }

    /**
     * The overloaded constructor.
     *
     * @param lfn The logical name of the file that has to be transferred.
     * @param job The name of the job with which the transfer is associated with.
     * @param flags the BitSet flags.
     */
    public FileTransfer(String lfn, String job, BitSet flags) {
        mLogicalFile = lfn;
        mJob = job;
        mSourceMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        mDestMap = new LinkedHashMap<String, List<ReplicaCatalogEntry>>();
        mFlags = (BitSet) flags.clone();
        this.mPriority = 0;
        this.mURLForRegistrationOnDestination = null;
        this.mVerifySymlinkSource = true;
    }

    /**
     * It returns the name of the main/compute job making up the VDS supernode with which this
     * transfer is related.
     *
     * @return the name of associated job
     */
    public String getJobName() {
        return this.mJob;
    }

    /**
     * Adds a source URL for the transfer.
     *
     * @param nv the NameValue object containing the name of the site as the key and URL as the
     *     value.
     */
    public void addSource(NameValue nv) {
        this.addSource(nv.getKey(), nv.getValue());
    }

    /**
     * Adds a source URL for the transfer.
     *
     * @param site the site from which the source file is being transferred.
     * @param url the source url.
     */
    public void addSource(String site, String url) {
        this.addSource(new ReplicaCatalogEntry(url, site));
    }

    /**
     * Adds a source URL for the transfer.
     *
     * @param rce ReplicaCatalogEntry object
     */
    public void addSource(ReplicaCatalogEntry rce) {
        List<ReplicaCatalogEntry> l = null;
        if (mSourceMap.containsKey(rce.getResourceHandle())) {
            // add the url to the existing list
            l = (List) mSourceMap.get(rce.getResourceHandle());
            // add the entry to the list
            l.add(rce);
        } else {
            // add a new list
            l = new ArrayList(3);
            l.add(rce);
            mSourceMap.put(rce.getResourceHandle(), l);
        }
    }

    /**
     * Adds a destination URL for the transfer.
     *
     * @param nv the NameValue object containing the name of the site as the key and URL as the
     *     value.
     */
    public void addDestination(NameValue nv) {
        this.addDestination(nv.getKey(), nv.getValue());
    }

    /**
     * Adds a destination URL for the transfer.
     *
     * @param site the site to which the destination file is being transferred.
     * @param url the destination url.
     */
    public void addDestination(String site, String url) {
        this.addDestination(new ReplicaCatalogEntry(url, site));
    }

    /**
     * Adds a destination URL for the transfer.
     *
     * @param rce ReplicaCatalogEntry object
     */
    public void addDestination(ReplicaCatalogEntry rce) {
        List<ReplicaCatalogEntry> l = null;
        if (mDestMap.containsKey(rce.getResourceHandle())) {
            // add the url to the existing list
            l = (List) mDestMap.get(rce.getResourceHandle());
            // add the entry to the list
            l.add(rce);
        } else {
            // add a new list
            l = new ArrayList(3);
            l.add(rce);
            mDestMap.put(rce.getResourceHandle(), l);
        }
    }

    /**
     * Sets the registration url for the destination.
     *
     * @param url the url
     */
    public void setURLForRegistrationOnDestination(String url) {
        this.mURLForRegistrationOnDestination = url;
    }

    /**
     * Sets the registration url for the destination.
     *
     * @return the destination url
     */
    public String getURLForRegistrationOnDestination() {
        return this.mURLForRegistrationOnDestination;
    }

    /**
     * Sets the priority for the File Transfer
     *
     * @param priority the priority associated with the FileTransfer
     */
    public void setPriority(int priority) {
        this.mPriority = priority;
    }

    /**
     * Sets the priority for the File Transfer
     *
     * @return the priority associated with the FileTransfer
     */
    public int getPriority() {
        return this.mPriority;
    }

    /**
     * Sets the transfer to not fail on non existent source
     *
     * @param verify the force associated with the FileTransfer
     */
    public void setVerifySymlinkSource(boolean verify) {
        this.mVerifySymlinkSource = verify;
    }

    /**
     * Returns boolean indicating whether to fail on non existent source or not
     *
     * @return boolean
     */
    public boolean verifySymlinkSource() {
        return this.mVerifySymlinkSource;
    }

    /**
     * Returns all the sites where the LFN exists
     *
     * @return Collection of site names
     */
    public Collection<String> getSourceSites() {
        return mSourceMap.keySet();
    }

    /**
     * Returns all the source URLS associated with the transfer object for a particular site
     *
     * @return List<ReplicaCatalogEntry> urls
     */
    public List<ReplicaCatalogEntry> getSourceURLs(String site) {
        return (mSourceMap.containsKey(site)) ? mSourceMap.get(site) : new ArrayList();
    }

    /**
     * Returns number of source URL's associated with the FileTransfer
     *
     * @return count
     */
    public int getSourceURLCount() {
        int count = 0;
        for (String site : getSourceSites()) {
            count += this.getSourceURLs(site).size();
        }
        return count;
    }

    /**
     * Returns a single source url associated with the transfer. The source url returned is first
     * entry from the key set of the underlying map.
     *
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue getSourceURL() {
        return getSourceURL(false);
    }

    /**
     * Returns a single source url associated with the transfer. If random is set to false,
     * thensource url returned is first entry from the key set of the underlying map.
     *
     * @param random boolean indicating if a random entry needs to be picked.
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue getSourceURL(boolean random) {
        return getURL(mSourceMap, random);
    }

    /**
     * Returns a single destination url associated with the transfer. The destination url returned
     * is first entry from the key set of the underlying map.
     *
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue getDestURL() {
        return getDestURL(false);
    }

    /**
     * Returns a single destination url associated with the transfer. If random is set to false,
     * then dest url returned is first entry from the key set of the underlying map.
     *
     * @param random boolean indicating if a random entry needs to be picked.
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue getDestURL(boolean random) {
        return getURL(mDestMap, random);
    }

    /**
     * Removes a single source url associated with the transfer. The source url removed is first
     * entry from the key set of the underlying map.
     *
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue removeSourceURL() {
        return removeURL(mSourceMap);
    }

    /**
     * Removes a single destination url associated with the transfer. The destination url removed is
     * first entry from the key set of the underlying map.
     *
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    public NameValue removeDestURL() {
        return removeURL(mDestMap);
    }

    /**
     * Returns a boolean indicating if a file that is being staged is an executable or not (i.e is a
     * data file).
     *
     * @return boolean indicating whether a file is executable or not.
     */
    public boolean isTransferringExecutableFile() {
        return this.isExecutable();
    }

    /**
     * Returns a boolean indicating if a file that is being staged is a container or not.
     *
     * @return boolean indicating whether a file is container or not.
     */
    public boolean isTransferringContainer() {
        return this.isContainerFile();
    }

    /**
     * Returns a single url from the map passed. If the random parameter is set, then a random url
     * is returned from the values for the first site.
     *
     * <p>Fix Me: Random set to true, shud also lead to randomness on the sites.
     *
     * @param m the map containing the url's
     * @param random boolean indicating that a random url to be picked up.
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    private NameValue getURL(Map<String, List<ReplicaCatalogEntry>> m, boolean random) {
        if (m == null || m.keySet().isEmpty()) {
            return null;
        }

        // Return the first url from the EntrySet
        Iterator it = m.entrySet().iterator();
        Map.Entry entry = (Map.Entry) it.next();
        List<ReplicaCatalogEntry> urls = (List) entry.getValue();
        String site = (String) entry.getKey();

        ReplicaCatalogEntry rce =
                (random)
                        ?
                        // pick a random value
                        urls.get(PegRandom.getInteger(0, urls.size() - 1))
                        :
                        // returning the first element. No need for a check as
                        // population of the list is controlled
                        urls.get(0);

        return (rce == null) ? null : new NameValue(rce.getResourceHandle(), rce.getPFN());
    }

    /**
     * Removes a single url from the map passed.
     *
     * @param m the map containing the url's
     * @return NameValue where the name would be the site on which the URL is and value the URL.
     *     null if no urls are assoiciated with the object.
     */
    private NameValue removeURL(Map<String, List<ReplicaCatalogEntry>> m) {
        if (m == null || m.keySet().isEmpty()) {
            return null;
        }

        // Return the first url from the EntrySet
        Iterator it = m.entrySet().iterator();
        Map.Entry<String, List<ReplicaCatalogEntry>> entry = (Map.Entry) it.next();
        // remove this entry
        it.remove();
        // returning the first element. No need for a check as
        // population of the list is controlled
        return new NameValue(entry.getKey(), entry.getValue().get(0).getPFN());
    }

    /**
     * Returns a clone of the object.
     *
     * @return clone of the object.
     */
    public Object clone() {
        FileTransfer ft = new FileTransfer();
        ft.mLogicalFile = new String(this.mLogicalFile);
        ft.mFlags = (BitSet) this.mFlags.clone();
        ft.mTransferFlag = this.mTransferFlag;
        ft.mSize = this.mSize;
        ft.mType = this.mType;
        ft.mJob = new String(this.mJob);
        ft.mPriority = this.mPriority;
        ft.mURLForRegistrationOnDestination = this.mURLForRegistrationOnDestination;
        ft.mMetadata = (Metadata) this.mMetadata.clone();
        ft.mVerifySymlinkSource = this.mVerifySymlinkSource;
        // the maps are not cloned underneath

        return ft;
    }

    /**
     * Determines whether the transfer contained in this container is valid or not. It is deemed
     * valid if there is at least one source url and one destination url.
     *
     * @return true if valid, else false.
     */
    public boolean isValid() {
        return !(mSourceMap.isEmpty() || mDestMap.isEmpty());
    }

    /**
     * Returns a textual interpretation of the object. The method outputs in a T2 compatible format.
     * Each FileTransfer object can refer to one section in the T2 format.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        String mode = (mTransferFlag == FileTransfer.TRANSFER_OPTIONAL) ? "optional" : "any";

        Iterator it = null;
        Map.Entry<String, List<ReplicaCatalogEntry>> entry = null;
        List l = null;

        sb.append(mLogicalFile).append(" ").append(mode);

        // writing out all the sources
        it = mSourceMap.entrySet().iterator();
        // sb.append("\n").append(" ");
        while (it.hasNext()) {
            entry = (Map.Entry) it.next();
            // inserting the source site
            sb.append("\n").append("#").append(entry.getKey());
            l = (List<ReplicaCatalogEntry>) entry.getValue();
            Iterator it1 = l.iterator();
            while (it1.hasNext()) {
                // write out the source url's
                // each line starts with a single whitespace
                sb.append("\n").append(" ").append(it1.next());
            }
        }

        // writing out all the destinations
        it = mDestMap.entrySet().iterator();
        // sb.append("\n").append(" ");
        while (it.hasNext()) {
            entry = (Map.Entry) it.next();
            // inserting the destination site
            sb.append("\n").append("# ").append(entry.getKey());
            l = (List<ReplicaCatalogEntry>) entry.getValue();
            Iterator it1 = l.iterator();
            while (it1.hasNext()) {
                // write out the source url's
                // each line starts with a two whitespaces
                sb.append("\n").append(" ").append(" ").append(it1.next());
            }
        }

        return sb.toString();
    }

    /**
     * Assimilates all metadata including checksum related data from the transformation catalog
     * entry object.
     *
     * @param entry
     */
    public void assimilateChecksum(TransformationCatalogEntry entry) {
        if (entry.hasCheckSum()) {
            // PM-1617 add all metadata from the entry into FileTransfer
            Metadata m = (Metadata) entry.getAllProfiles().get(Profiles.NAMESPACES.metadata);
            this.getAllMetadata().merge(m);
        }
    }
}
