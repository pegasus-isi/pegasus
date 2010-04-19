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

package edu.isi.pegasus.planner.catalog.transformation.classes;

/**
 * This is an enumerated data class for the different types of transformation.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */

import java.io.Serializable;
import java.util.HashMap;

public class TCType
    implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(6);

    protected TCType(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    public static final String _STATIC_BINARY = "STATIC_BINARY";
    public static final String _DYNAMIC_BINARY = "DYNAMIC_BINARY";
    public static final String _INSTALLED = "INSTALLED";
    public static final String _SOURCE = "SOURCE";
    public static final String _PACMAN_PACKAGE = "PACMAN_PACKAGE";
    public static final String _SCRIPT = "SCRIPT";

    public static final TCType STATIC_BINARY = new TCType(_STATIC_BINARY);
    public static final TCType DYNAMIC_BINARY = new TCType(_DYNAMIC_BINARY);
    public static final TCType INSTALLED = new TCType(_INSTALLED);
    public static final TCType SOURCE = new TCType(_SOURCE);
    public static final TCType PACMAN_PACKAGE = new TCType(_PACMAN_PACKAGE);
    public static final TCType SCRIPT = new TCType(_SCRIPT);

    public static final String err = "Error : Illegal TCType defined. Please specify one of the predefined types \n [INSTALLED, SOURCE, STATIC_BINARY, DYNAMIC_BINARY, PACMAN_PACKAGE, SCRIPT]";

    /**
     * Returns the string value of the type of transformation.
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates an objct of TCType given a string.
     * @param value String The type value as string
     * @throws IllegalStateException Throws Exception if the type is not defined in this class
     * @return TCType
     */
    public static TCType fromValue(String value) throws IllegalStateException {
        TCType m_enum = (TCType) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates an object of TCType given a string.
     * @param value String The type value as string.
     * @throws IllegalStateException Throws Exception if the type is not defined in this class.
     * @return TCType
     */
    public static TCType fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given TCType object equals this TCType object.
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
     * Returns the string value of the type of transformation.
     * @return String
     */
    public String toString() {
        return _value_;
    }

}
