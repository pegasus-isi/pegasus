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
package edu.isi.pegasus.planner.invocation;

/**
 * The uname element.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Uname extends MachineInfo implements HasText {

    /** The element name */
    public static final String ELEMENT_NAME = "uname";

    /** The system attribute for the uname */
    public static final String SYSTEM_ATTRIBUTE_KEY = "system";

    /** The nodename attribute for the uname */
    public static final String NODENAME_ATTRIBUTE_KEY = "nodename";

    /** The release attribute for the uname */
    public static final String RELEASE_ATTRIBUTE_KEY = "release";

    /** The release attribute for the uname */
    public static final String MACHINE_ATTRIBUTE_KEY = "machine";

    /** The archmode attribute for the uname */
    public static final String ARCHMODE_ATTRIBUTE_KEY = "archmode";

    /** The undefined value for archmode attribute. */
    public static final String UNDEFINED_ARCHMODE_VALUE = "UNDEFINED";

    /**
     * An adapter method to convert the uname object to the architecture object.
     *
     * @param uname the object to be converted
     * @return the converted <code>Architecture</code> object
     */
    public static Architecture unameToArchitecture(Uname uname) {
        Architecture arch = new Architecture();
        arch.setMachine(uname.get(Uname.MACHINE_ATTRIBUTE_KEY));
        arch.setNodeName(uname.get(Uname.NODENAME_ATTRIBUTE_KEY));
        arch.setRelease(uname.get(Uname.RELEASE_ATTRIBUTE_KEY));
        arch.setSystemName(uname.get(Uname.SYSTEM_ATTRIBUTE_KEY));
        arch.setArchMode(
                uname.get(Uname.ARCHMODE_ATTRIBUTE_KEY) == null
                        ? Uname.UNDEFINED_ARCHMODE_VALUE
                        : uname.get(Uname.ARCHMODE_ATTRIBUTE_KEY));
        arch.setValue(uname.getValue());
        return arch;
    }

    /** The text value */
    private StringBuffer mValue;

    /** The default constructor */
    public Uname() {
        super();
        mValue = null;
    }

    /**
     * Constructs a piece of data.
     *
     * @param value is the data to remember. The string may be empty, but it must not be <code>null
     *     </code>.
     * @exception NullPointerException if the argument was null.
     */
    public Uname(String value) {
        this();
        if (value == null) {
            throw new NullPointerException(
                    "the value to the <data> tag constructor must not be null");
        } else {
            mValue = new StringBuffer(value);
        }
    }

    /**
     * Returns the name of the xml element corresponding to the object.
     *
     * @return name
     */
    public String getElementName() {
        return ELEMENT_NAME;
    }

    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment) {
        if (fragment != null) {
            if (this.mValue == null) {
                this.mValue = new StringBuffer(fragment);
            } else {
                this.mValue.append(fragment);
            }
        }
    }

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue() {
        return (mValue == null ? null : mValue.toString());
    }

    /**
     * Accessor.
     *
     * @param value is the new value to set.
     * @see #getValue()
     */
    public void setValue(String value) {
        this.mValue = (value == null ? null : new StringBuffer(value));
    }

    /**
     * An adapter method to convert the uname object to the architecture object.
     *
     * @return the converted <code>Architecture</code> object
     */
    public Architecture toArchitecture() {
        return Uname.unameToArchitecture(this);
    }
}
