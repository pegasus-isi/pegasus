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
 * This is an enumerated data class for the different types of architecture.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
import java.io.Serializable;
import java.util.HashMap;

public class Arch implements Serializable {
    private String _value_;
    private static HashMap _table_ = new HashMap(5);

    protected Arch(String value) {
        _value_ = value;
        _table_.put(_value_, this);
    }

    private static final String _INTEL32 = "INTEL32";
    private static final String _INTEL64 = "INTEL64";
    private static final String _SPARCV7 = "SPARCV7";
    private static final String _SPARCV9 = "SPARCV9";
    private static final String _AMD64 = "AMD64";

    public static final Arch INTEL32 = new Arch(_INTEL32);
    public static final Arch INTEL64 = new Arch(_INTEL64);
    public static final Arch SPARCV7 = new Arch(_SPARCV7);
    public static final Arch SPARCV9 = new Arch(_SPARCV9);
    public static final Arch AMD64 = new Arch(_AMD64);

    public static final String err =
            "Error: Illegal Architecture defined. Please specify one of the predefined types \n [INTEL32, INTEL64, AMD64, SPARCV7, SPARCV9]";

    /**
     * Returns the value of the architecture as string.
     *
     * @return String
     */
    public String getValue() {
        return _value_;
    }

    /**
     * Creates a new Arch Object givan a arch string.
     *
     * @param value String
     * @throws IllegalStateException Throws Exception if the architecure is not defined in this
     *     class.
     * @return Arch
     */
    public static Arch fromValue(String value) throws IllegalStateException {
        Arch m_enum = (Arch) _table_.get(value.toUpperCase());
        if (m_enum == null) {
            throw new IllegalStateException(err);
        }
        return m_enum;
    }

    /**
     * Creates a new Arch object given a arch string.
     *
     * @param value String
     * @throws IllegalStateException Throws Exception if the architecure is not defined in this
     *     class.
     * @return Arch
     */
    public static Arch fromString(String value) throws IllegalStateException {
        return fromValue(value);
    }

    /**
     * Compares if a given Arch object is equal to this.
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
     * Returns the string value of the architecture.
     *
     * @return String
     */
    public String toString() {
        return _value_;
    }
}
