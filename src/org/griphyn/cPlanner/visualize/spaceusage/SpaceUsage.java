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

package org.griphyn.cPlanner.visualize.spaceusage;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Iterator;

/**
 * A container object that stores the space usage for each site.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SpaceUsage {

    /**
     * The map that stores the list of space objects indexed by site name.
     */
    private Map mStore;

    /**
     * The default store.
     */
    public SpaceUsage() {
        mStore = new HashMap();
    }


    /**
     * Returns an iterator to list of <code>String</code> site identifiers
     * for which data is available.
     *
     * @return Iterator
     */
    public Iterator siteIterator(){
        return mStore.keySet().iterator();
    }

    /**
     * Returns the list of <code>Space</code> objects corresponding to a
     * particular site.
     *
     * @param site  the site for which sizes are required.
     *
     * @return List
     */
    public List getSizes( String site ) {
        return (mStore.containsKey( site ) ?  (List) mStore.get( site ) : new LinkedList());
    }
    /**
     * Add a Space record to the store.
     *
     * @param site     the site for which the record is logged.
     * @param record   the <code>SpaceUsage</code> record.
     */
    public void addRecord( String site, Space record ){
        List l =  ( mStore.containsKey( site ) ) ?
                                                (List) mStore.get( site ):
                                                new LinkedList();
        l.add( record );
        mStore.put( site, l );
    }


    /**
     * Sorts the records for each site.
     */
    public void sort(){
        SpaceComparator s = new SpaceComparator();
        for( Iterator it = mStore.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            List l = (List)entry.getValue();
            Collections.sort( l, s );
        }
    }

    /**
     * Returns textual description of the object.
     *
     * @return the textual description
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( "{\n ");
        for( Iterator it = mStore.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            List l = (List)entry.getValue();
            sb.append( entry.getKey() ).append( " -> " );
            for( Iterator lIT = l.iterator(); lIT.hasNext(); ){
                sb.append( "\n\t");
                sb.append( lIT.next() );
                sb.append( " , ");
            }
        }
        sb.append( "\n}" );
        return sb.toString();
    }
}


/**
 * Comparator for Space objects that allows us to sort on time.
 *
 */
class SpaceComparator implements Comparator{

/**
   * Implementation of the {@link java.lang.Comparable} interface.
   * Compares this object with the specified object for order. Returns a
   * negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object. The
   * definitions are compared by their type, and by their short ids.
   *
   * @param o1 is the object to be compared
   * @param o2 is the object to be compared with o1.
   *
   * @return a negative number, zero, or a positive number, if the
   * object compared against is less than, equals or greater than
   * this object.
   * @exception ClassCastException if the specified object's type
   * prevents it from being compared to this Object.
   */
  public int compare( Object o1, Object o2 )
  {
    if ( o1 instanceof Space && o2 instanceof Space ) {
      Space s1 = (Space) o1;
      Space s2 = (Space) o2;

      return s1.getDate().compareTo( s2.getDate() );
    } else {
      throw new ClassCastException( "object is not a Space" );
    }
  }
}
