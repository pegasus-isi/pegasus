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

package org.griphyn.cPlanner.classes;


import org.griphyn.common.catalog.ReplicaCatalogEntry;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A Data Class that associates a LFN with the PFN's. Attributes associated
 * with the LFN go here.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 *
 * @see org.griphyn.common.catalog.ReplicaCatalogEntry
 */
public class ReplicaLocation
       extends Data
       implements Cloneable{


    /**
     * The site name that is associated in the case the resource handle is not
     * specified with the PFN.
     */
    public static final String UNDEFINED_SITE_NAME = "UNDEFINED_SITE";

    /**
     * The LFN associated with the entry.
     */
    private String mLFN;

    /**
     * A list of <code>ReplicaCatalogEntry</code> objects containing the PFN's
     * and associated attributes.
     */
    private List mPFNList;

    /**
     * Default constructor.
     */
    public ReplicaLocation(){
        mLFN     = new String();
        mPFNList = new ArrayList();
    }

    /**
     * Overloaded constructor.
     * Intializes the member variables to the values passed.
     *
     * @param lfn  the logical filename.
     * @param pfns the list of <code>ReplicaCatalogEntry</code> objects.
     */
    public ReplicaLocation( String lfn , List pfns ){
        mLFN     = lfn;
        mPFNList = pfns;
        //sanitize pfns. add a default resource handle if not specified
        sanitize( mPFNList );
    }

    /**
     * Overloaded constructor.
     * Intializes the member variables to the values passed.
     *
     * @param lfn  the logical filename.
     * @param pfns the list of <code>ReplicaCatalogEntry</code> objects.
     */
    public ReplicaLocation( String lfn , Collection pfns ){
        mLFN     = lfn;

        //create a separate list only if required
        mPFNList = ( pfns instanceof List )?
                   (List)pfns:
                   //create a new list from the collection
                   new ArrayList( pfns ) ;

        //sanitize pfns. add a default resource handle if not specified
        sanitize( mPFNList );

    }


    /**
     * Add a PFN and it's attributes. Any existing
     * mapping with the same PFN will be replaced, including all its
     * attributes.
     *
     * @param tuple  the <code>ReplicaCatalogEntry</code> object containing the
     *             PFN and the attributes.
     */
    public void addPFN( ReplicaCatalogEntry tuple ){
        boolean seen = false;
        String pfn = tuple.getPFN();

        sanitize( tuple );

        //traverse through the existing PFN's to check for the
        //same pfn
        for ( Iterator i= this.pfnIterator(); i.hasNext() && ! seen; ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) i.next();
            if ( (seen = pfn.equals(rce.getPFN())) ) {
                try {
                    i.remove();
                } catch ( UnsupportedOperationException uoe ) {
                    //ignore for time being
                }
            }
        }

        this.mPFNList.add( tuple );

    }

    /**
     * Add a PFN and it's attributes.
     *
     * @param tuples  the <code>List</code> object of <code>ReplicaCatalogEntry</code>
     *              objects, each containing the  PFN and the attributes.
     */
    protected void addPFNs( List tuples ){
        for( Iterator it = tuples.iterator(); it.hasNext(); ){
            addPFN( (ReplicaCatalogEntry)it.next() );
        }
    }


    /**
     * Sets the LFN.
     *
     * @param lfn  the lfn.
     */
    public void setLFN( String lfn ){
        this.mLFN = lfn;
    }

    /**
     * Returns the associated LFN.
     *
     * @return lfn
     */
    public String getLFN( ){
        return this.mLFN;
    }

    /**
     * Return a PFN as a <code>ReplicaCatalogEntry</code>
     *
     * @param index   the pfn location.
     *
     * @return the element at the specified position in this list.
     *
     * @throws IndexOutOfBoundsException - if the index is out of range (index < 0 || index >= size()).
     */
    public ReplicaCatalogEntry getPFN( int index ){
        return (ReplicaCatalogEntry)this.mPFNList.get( index );
    }


    /**
     * Returns the list of pfn's as <code>ReplicaCatalogEntry</code> objects.
     *
     * @return List
     */
    public List getPFNList( ){
        return this.mPFNList;
    }


    /**
     * Returns an iterator to the list of <code>ReplicaCatalogEntry</code>
     * objects.
     *
     * @return Iterator.
     */
    public Iterator pfnIterator(){
        return this.mPFNList.iterator();
    }

    /**
     * Returns the number of pfn's associated with the lfn.
     *
     * @return int
     */
    public int getPFNCount(){
        return this.mPFNList.size();
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        ReplicaLocation rc;
        try{
            rc = ( ReplicaLocation ) super.clone();
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        rc.mPFNList = new ArrayList();
        rc.setLFN( this.mLFN );

        //add all the RCE's
        for( Iterator it = this.pfnIterator(); it.hasNext(); ){
            //creating a shallow clone here.
            rc.addPFN( ( ReplicaCatalogEntry )it.next() );
        }
        //clone is not implemented fully.
        //throw new RuntimeException( "Clone not implemented for " + this.getClass().getName() );
        return rc;
    }

    /**
     * Merges the <code>ReplicaLocation</code> object to the existing one,
     * only if the logical filenames match.
     *
     * @param location is another <code>ReplicaLocations</code> to merge with.
     *
     * @return true if a merge was successful, false if the LFNs did not
     * match.
     */
    public boolean merge(ReplicaLocation location){
        String lfn1 = this.getLFN();
        String lfn2 = (location == null)? null : location.getLFN();
        boolean result = (lfn1 == null && lfn2 == null ||
                          lfn1 != null && lfn2 != null && lfn1.equals(lfn2));

        // only merge if PFN match
        if (result) {
            this.addPFNs( location.getPFNList() );
        }

        return result;
    }




    /**
     * Returns the textual description of the data class.
     *
     * @return the textual description.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( mLFN ).append( " -> {");
        for( Iterator it = this.pfnIterator(); it.hasNext(); ){
            sb.append( it.next() );
            sb.append( "," );
        }
        sb.append( "}" );
        return sb.toString();
    }


    /**
     * Sanitizes a tuple list . Sets the resource handle to a default value if not
     * specified.
     *
     * @param tuples  the tuple to be sanitized.
     */
    private void sanitize( List tuples ){
        for( Iterator it = tuples.iterator(); it.hasNext(); ){
            this.sanitize( (ReplicaCatalogEntry)it.next() );
        }
    }


    /**
     * Sanitizes a tuple . Sets the resource handle to a default value if not
     * specified.
     *
     * @param tuple  the tuple to be sanitized.
     */
    private void sanitize( ReplicaCatalogEntry tuple ){
        //sanity check
        if( tuple.getResourceHandle() == null ){
            tuple.setResourceHandle( this.UNDEFINED_SITE_NAME );
        }
    }
}
