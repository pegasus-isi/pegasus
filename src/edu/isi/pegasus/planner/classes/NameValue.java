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
/**
 * The object of this class holds the name value pair. At present to be used for environment
 * variables. Will be used more after integration of Spitfire.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class NameValue<K, V> extends Data implements Comparable {

    /** stores the name of the pair. */
    private K name;

    /** stores the corresponding value to the name in the pair. */
    private V value;

    /** the default constructor which initialises the class member variables. */
    public NameValue() {
        name = null;
        value = null;
    }

    /**
     * Initialises the class member variables to the values passed in the arguments.
     *
     * @param name corresponds to the name in the NameValue pair.
     * @param value corresponds to the value for the name in the NameValue pair.
     */
    public NameValue(K name, V value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Sets the key associated with this tuple.
     *
     * @param key the key associated with the tuple.
     */
    public void setKey(K key) {
        this.name = key;
    }

    /**
     * Sets the value associated with this tuple.
     *
     * @param value the value associated with the tuple.
     */
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Returns the key associated with this tuple.
     *
     * @return the key associated with the tuple.
     */
    public K getKey() {
        return this.name;
    }

    /**
     * Returns the value associated with this tuple.
     *
     * @return value associated with the tuple.
     */
    public V getValue() {
        return this.value;
    }

    /**
     * Check if the system information matches.
     *
     * @param obj to be compared.
     * @return boolean
     */
    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof NameValue) {
            NameValue nv = (NameValue) obj;

            result = this.getKey().equals(nv.getKey()) && this.getValue().equals(nv.getValue());
        }
        return result;
    }

    /**
     * Returns a copy of this object
     *
     * @return object containing a cloned copy of the tuple.
     */
    public Object clone() {
        NameValue nv = new NameValue(this.name, this.value);
        return nv;
    }

    /**
     * Writes out the contents of the class to a String in form suitable for displaying.
     *
     * @return the textual description.
     */
    public String toString() {
        String str = this.getKey() + "=" + this.getValue();
        return str;
    }

    /**
     * Implementation of the {@link java.lang.Comparable} interface. Compares this object with the
     * specified object for order. Returns a negative integer, zero, or a positive integer as this
     * object is less than, equal to, or greater than the specified object. The NameValue are
     * compared by their keys.
     *
     * @param o is the object to be compared
     * @return a negative number, zero, or a positive number, if the object compared against is less
     *     than, equals or greater than this object.
     * @exception ClassCastException if the specified object's type prevents it from being compared
     *     to this Object.
     */
    public int compareTo(Object o) {
        if (o instanceof NameValue) {
            NameValue nv = (NameValue) o;
            if (this.name instanceof Comparable) {
                return ((Comparable) this.name).compareTo(nv.name);
            } else {
                throw new IllegalArgumentException(
                        "The key for the object "
                                + this
                                + " does not implment comparable interface");
            }
        } else {
            throw new ClassCastException("Object is not a NameValue");
        }
    }
}
