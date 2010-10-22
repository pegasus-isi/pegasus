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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
 */
public class Profile {

    public static enum NAMESPACE {

        condor, pegasus, dagman, globus, hints, selector, stat, env
    }
    protected String mNamespace;
    protected String mKey;
    protected String mValue;

    public Profile(String namespace, String key) {
        mNamespace = namespace;
        mKey = key;
    }

    public Profile(String namespace, String key, String value) {
        mNamespace = namespace;
        mKey = key;
        mValue = value;
    }

    public Profile(NAMESPACE namespace, String key, String value) {
        mNamespace = namespace.toString();
        mKey = key;
        mValue = value;
    }

    public Profile(Profile p) {
        this(p.getNameSpace(), p.getKey(), p.getValue());
    }

    public String getKey() {
        return mKey;
    }

    public String getNameSpace() {
        return mNamespace;
    }

    public String getValue() {
        return mValue;
    }

    public Profile setValue(String value) {
        mValue = value;
        return this;
    }

    @Override
    public Profile clone() {
        return new Profile(this.mNamespace, this.mKey, this.mValue);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("profile", indent).writeAttribute("namespace", mNamespace);
        writer.writeAttribute("key", mKey).writeData(mValue).endElement();

    }
}
