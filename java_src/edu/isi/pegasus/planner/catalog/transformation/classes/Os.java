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
package edu.isi.pegasus.planner.catalog.transformation.classes;

/**
 * This is an enumerated data class for the different types of operating systems.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
import java.io.Serializable;
import java.util.HashMap;

public class Os implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(5);

    protected Os(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    private static final String _LINUX = "LINUX";
    private static final String _SUNOS = "SUNOS";
    private static final String _AIX = "AIX";
    private static final String _WINDOWS = "WINDOWS";

    public static final Os LINUX = new Os(_LINUX);
    public static final Os SUNOS = new Os(_SUNOS);
    public static final Os AIX = new Os(_AIX);
    public static final Os WINDOWS = new Os(_WINDOWS);

    public static final String err =
            "Error: Illegal Operating System defined. Please specify one of the predefined types \n [LINUX, SUNOS, AIX, WINDOWS]";

    /**
     * Returns the value of the operating system as string.
     *
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates a new Os object given an os string.
     *
     * @param value String
     * @throws IllegalStateException Throws Exception if the operating system is not defined in this
     *     class.
     * @return Os
     */
    public static Os fromValue(String value) throws IllegalStateException {
        Os m_enum = (Os) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates a new Os object given an os string.
     *
     * @param value String
     * @throws IllegalStateException Throws Exception if the operating system is not defined in this
     *     class.
     * @return Os
     */
    public static Os fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given Os object is equal to this.
     *
     * @param obj Object
     * @return boolean
     */
    public boolean equals(Object obj) {
        return (obj == this);
    }

    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Returns the string value of the operating system.
     *
     * @return String
     */
    public String toString() {
        return _value_;
    }
}
