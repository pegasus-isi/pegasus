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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.util.XMLWriter;

/**
 * Profile Object for the DAX API
 *
 * @author gmehta
 * @version $Revision$
 */
public class Profile {

    /** Supported NAMESPACES. */
    public static enum NAMESPACE {
        CONDOR,
        condor,
        PEGASUS,
        pegasus,
        DAGMAN,
        dagman,
        GLOBUS,
        globus,
        HINTS,
        hints,
        SELECTOR,
        selector,
        STAT,
        stat,
        ENV,
        env
    }
    /** Namespace of the profile */
    protected String mNamespace;
    /** Key of the profile */
    protected String mKey;
    /** Value of the profile */
    protected String mValue;

    /**
     * Create a new Profile object
     *
     * @param namespace
     * @param key
     */
    public Profile(String namespace, String key) {
        mNamespace = namespace;
        mKey = key;
    }

    /**
     * Create a new Profile object
     *
     * @param namespace
     * @param key
     * @param value
     */
    public Profile(String namespace, String key, String value) {
        mNamespace = namespace;
        mKey = key;
        mValue = value;
    }

    /**
     * @param namespace
     * @param key
     * @param value
     */
    public Profile(NAMESPACE namespace, String key, String value) {
        mNamespace = namespace.toString();
        mKey = key;
        mValue = value;
    }

    /**
     * Copy constructor
     *
     * @param p
     */
    public Profile(Profile p) {
        this(p.getNameSpace(), p.getKey(), p.getValue());
    }

    /**
     * Get the key of this Profile
     *
     * @return
     */
    public String getKey() {
        return mKey;
    }

    /**
     * Get the namespace of this profile
     *
     * @return
     */
    public String getNameSpace() {
        return mNamespace;
    }

    /**
     * Get the value of this profile
     *
     * @return
     */
    public String getValue() {
        return mValue;
    }

    /**
     * Set the value of this Profile
     *
     * @param value
     * @return
     */
    public Profile setValue(String value) {
        mValue = value;
        return this;
    }

    /**
     * Create a copy of this Profile
     *
     * @return
     */
    @Override
    public Profile clone() {
        return new Profile(this.mNamespace, this.mKey, this.mValue);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("profile", indent)
                .writeAttribute("namespace", mNamespace.toLowerCase());
        writer.writeAttribute("key", mKey).writeData(mValue).endElement();
    }
}
