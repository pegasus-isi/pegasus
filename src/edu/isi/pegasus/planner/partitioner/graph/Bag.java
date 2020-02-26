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
package edu.isi.pegasus.planner.partitioner.graph;

/**
 * An interface to define a BAG of objects. The bag can be then associated with other data
 * structures, like Graph Nodes.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Bag {

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key the key corresponding to which the objects need to be returned.
     * @return the object that is found corresponding to the key or null.
     */
    public Object get(Object key);

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     * @return boolean indicating if insertion was successful.
     */
    public boolean add(Object key, Object value);

    /**
     * Returns true if the namespace contains a mapping for the specified key. More formally,
     * returns true if and only if this map contains at a mapping for a key k such that (key==null ?
     * k==null : key.equals(k)). (There can be at most one such mapping.)
     *
     * @param key The key that you want to search for in the bag.
     */
    public boolean containsKey(Object key);
}
