/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.classes;


import edu.isi.pegasus.planner.common.PegRandom;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is a container for the storing the transfers that are required in
 * between sites. It refers to one lfn, but can contains more than one source
 * and destination urls. All the source url's are presumed to be identical.
 * The destination urls, can in effect be used to refer to TFN's for a lfn on
 * different pools.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */
public class FileTransfer extends PegasusFile {

    /**
     * The logical name of the asssociated VDS super node, with which the file
     * is associated. The name of the job can be of the job that generates that
     * file(while doing interpool or transferring output files to output pool)
     * or of a job for which the file is an input(getting an input file from the
     * Replica Services).
     */
    private String mJob;

    /**
     * The map containing all the source urls keyed by the pool id/name.
     * Corresponding to each pool, a list of url's is stored that contain
     * the URL's for that pool. All url's not associated with a pool, are
     * associated with a undefined pool.
     */
    private Map mSourceMap;

    /**
     * The map containing all the destination urls keyed by the pool id/name.
     * Corresponding to each pool, a list of url's is stored that contain
     * the URL's for that pool. All url's not associated with a pool, are
     * associated with a undefined pool.
     */
    private Map mDestMap;

    /**
     * Default constructor.
     */
    public FileTransfer(){
        super();
        mJob         = new String();
        mFlags       = new BitSet(NO_OF_TRANSIENT_FLAGS);
        mSourceMap   = new HashMap();
        mDestMap     = new HashMap();
    }

    /**
     * The overloaded constructor.
     *
     * @param pf  <code>PegasusFile</code> object containing the transiency
     *            attributes, and the logical name of the file.
     */
    public FileTransfer(PegasusFile pf){
        this.mLogicalFile  = pf.mLogicalFile;
        this.mTransferFlag = pf.mTransferFlag;
        this.mSize         = pf.mSize;
        this.mFlags        = pf.getFlags();
        this.mType         = pf.getType();
        this.mJob          = new String();
        this.mSourceMap    = new HashMap();
        this.mDestMap      = new HashMap();

    }

    /**
     * The overloaded constructor.
     *
     * @param lfn        The logical name of the file that has to be transferred.
     * @param job        The name of the job with which the transfer is
     *                   associated with.
     */
    public FileTransfer(String lfn, String job){
        super(lfn);
        mJob         = job;
        mSourceMap   = new HashMap();
        mDestMap     = new HashMap();
        mFlags       = new BitSet(NO_OF_TRANSIENT_FLAGS);
    }

    /**
     * The overloaded constructor.
     *
     * @param lfn        The logical name of the file that has to be transferred.
     * @param job        The name of the job with which the transfer is
     *                   associated with.
     * @param flags      the BitSet flags.
     */
    public FileTransfer(String lfn, String job, BitSet flags){

        mLogicalFile = lfn;
        mJob         = job;
        mSourceMap   = new HashMap();
        mDestMap     = new HashMap();
        mFlags       = (BitSet)flags.clone();
    }


    /**
     * It returns the name of the main/compute job making up the VDS supernode
     * with which this transfer is related.
     *
     * @return the name of associated job
     */
    public String getJobName(){
        return this.mJob;
    }

    /**
     * Adds a source URL for the transfer.
     *
     * @param nv  the NameValue object containing the name of the site as the key
     *            and URL as the value.
     */
    public void addSource(NameValue nv){
        this.addSource(nv.getKey(),nv.getValue());
    }



    /**
     * Adds a source URL for the transfer.
     *
     * @param pool  the pool from which the source file is being transferred.
     * @param url   the source url.
     */
    public void addSource(String pool, String url){
        List l = null;
        if(mSourceMap.containsKey(pool)){
            //add the url to the existing list
            l = (List)mSourceMap.get(pool);
            //add the entry to the list
            l.add(url);
        }
        else{
            //add a new list
            l = new ArrayList(3);
            l.add(url);
            mSourceMap.put(pool,l);
        }
    }

    /**
     * Adds a destination URL for the transfer.
     *
     * @param nv  the NameValue object containing the name of the site as the key
     *            and URL as the value.
     */
    public void addDestination(NameValue nv){
        this.addDestination(nv.getKey(),nv.getValue());
    }


    /**
     * Adds a destination URL for the transfer.
     *
     * @param pool  the pool to which the destination file is being transferred.
     * @param url   the destination url.
     */
    public void addDestination(String pool, String url){
        List l = null;
        if(mDestMap.containsKey(pool)){
            //add the url to the existing list
            l = (List)mDestMap.get(pool);
            //add the entry to the list
            l.add(url);
        }
        else{
            //add a new list
            l = new ArrayList(3);
            l.add(url);
            mDestMap.put(pool,l);
        }

    }


    /**
     * Returns a single source url associated with the transfer.
     * The source url returned is first entry from the key set of the
     * underlying map.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue getSourceURL(){
        return getSourceURL( false );
    }

    /**
     * Returns a single source url associated with the transfer.
     * If random is set to false, thensource url returned is first entry from
     * the key set of the  underlying map.
     *
     * @param random   boolean indicating if a random entry needs to be picked.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue getSourceURL( boolean random ){
        return getURL( mSourceMap , random );
    }



    /**
     * Returns a single destination url associated with the transfer.
     * The destination url returned is first entry from the key set of the
     * underlying map.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue getDestURL(){
        return getDestURL( false );
    }


    /**
     * Returns a single destination url associated with the transfer.
     * If random is set to false, then dest url returned is first entry from
     * the key set of the  underlying map.
     *
     * @param random   boolean indicating if a random entry needs to be picked.

     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue getDestURL( boolean random ){
        return getURL( mDestMap, random );
    }



    /**
     * Removes a single source url associated with the transfer.
     * The source url removed is first entry from the key set of the
     * underlying map.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue removeSourceURL(){
        return removeURL(mSourceMap);
    }


    /**
     * Removes a single destination url associated with the transfer.
     * The destination url removed is first entry from the key set of the
     * underlying map.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    public NameValue removeDestURL(){
        return removeURL(mDestMap);
    }


    /**
     * Returns a boolean indicating if a file that is being staged is an
     * executable or not (i.e is a data file).
     *
     * @return boolean indicating whether a file is executable or not.
     */
    public boolean isTransferringExecutableFile(){
        return (this.mType == this.EXECUTABLE_FILE);
    }

    /**
     * Returns a single url from the map passed. If the random parameter is set,
     * then a random url is returned from the values for the first site.
     *
     * Fix Me: Random set to true, shud also lead to randomness on the sites.
     *
     * @param m       the map containing the url's
     * @param random  boolean indicating that a random url to be picked up.
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    private NameValue getURL( Map m, boolean random ){
        if(m == null || m.keySet().isEmpty()){
            return null;
        }

        //Return the first url from the EntrySet
        Iterator it = m.entrySet().iterator();
        Map.Entry entry = ( Map.Entry )it.next();
        List urls       = ( List )entry.getValue();
        String site     = ( String )entry.getKey();


        return ( random ) ?
                //pick a random value
                new NameValue( site, ( String ) urls.get( PegRandom.getInteger( 0, urls.size() -1 )) ):
                //returning the first element. No need for a check as
                //population of the list is controlled
                new NameValue( site, ( String )( urls.get(0) ) );

    }

    /**
     * Removes a single url from the map passed.
     *
     * @param m  the map containing the url's
     *
     * @return NameValue where the name would be the pool on which the URL is
     *         and value the URL.
     *         null if no urls are assoiciated with the object.
     */
    private NameValue removeURL(Map m){
        if(m == null || m.keySet().isEmpty()){
            return null;
        }

        //Return the first url from the EntrySet
        Iterator it = m.entrySet().iterator();
        Map.Entry entry = (Map.Entry)it.next();
        //remove this entry
        it.remove();
        //returning the first element. No need for a check as
        //population of the list is controlled
        return new NameValue(
                             (String)entry.getKey(),
                             (String)( ((List)entry.getValue()).get(0) )
                             );

    }




    /**
     * Constructs a URL with the prefix as the poolname enclosed in #.
     *
     * @param site       the site
     * @param directory  the directory
     * @param filename   the filename
     *
     * @return String
     */
    private String constructURL(String site, String directory, String filename ){
        StringBuffer sb = new StringBuffer();
        sb/*.append("#").append(pool).append("#\n")*/
            .append( directory ).append(File.separatorChar).append(filename);

        return sb.toString();
    }

    /**
     * Returns a boolean value of whether the source url and the destination
     * url members of this object match or not.
     */
    /*public boolean URLsMatch(){
        if(mSourceURL.trim().equalsIgnoreCase(mDestURL.trim())){
            return true;
        }
        return false;
    }*/


    /**
     * Returns a clone of the object.
     *
     * @return clone of the object.
     */
    public Object clone() {
        FileTransfer ft = new FileTransfer();
        ft.mLogicalFile = new String(this.mLogicalFile);
        ft.mFlags        = (BitSet)this.mFlags.clone();
        ft.mTransferFlag = this.mTransferFlag;
        ft.mJob         = new String(this.mJob);

        //the maps are not cloned underneath

        return ft;
    }

    /**
     * Determines whether the transfer contained in this container is valid or
     * not. It is deemed valid if there is at least one source url and one
     * destination url.
     *
     * @return true if valid, else false.
     */
    public boolean isValid(){
        return !(mSourceMap.isEmpty() || mDestMap.isEmpty());
    }

    /**
     * Returns a textual interpretation of the object. The method outputs
     * in a T2 compatible format. Each FileTransfer object can refer to one
     * section in the T2 format.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        String mode = (mTransferFlag == this.TRANSFER_OPTIONAL)?
                      "optional" :
                      "any";

        Iterator it = null;
        Map.Entry entry = null;
        List l      = null;

        sb.append(mLogicalFile).append(" ").append(mode);

        //writing out all the sources
        it = mSourceMap.entrySet().iterator();
        //sb.append("\n").append(" ");
        while(it.hasNext()){
            entry = (Map.Entry) it.next();
            //inserting the source pool
            sb.append("\n").append("#").append(entry.getKey());
            l = (List)entry.getValue();
            Iterator it1 = l.iterator();
            while(it1.hasNext()){
                //write out the source url's
                //each line starts with a single whitespace
                sb.append("\n").append(" ").append(it1.next());
            }
        }

        //writing out all the destinations
        it = mDestMap.entrySet().iterator();
        //sb.append("\n").append(" ");
        while(it.hasNext()){
            entry = (Map.Entry) it.next();
            //inserting the destination pool
            sb.append("\n").append("# ").append(entry.getKey());
            l = (List)entry.getValue();
            Iterator it1 = l.iterator();
            while(it1.hasNext()){
                //write out the source url's
                //each line starts with a two whitespaces
                sb.append("\n").append(" ").append(" ").append(it1.next());
            }
        }

        return sb.toString();
    }

}
