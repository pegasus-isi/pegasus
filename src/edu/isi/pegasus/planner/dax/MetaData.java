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
public class MetaData {

    protected String mKey;
    protected String mType;
    protected String mValue;

    public MetaData(MetaData m) {
        //create a copy
        this(m.getKey(), m.getType(), m.getValue());
    }

    public MetaData(String type, String key) {
        mType = type;
        mKey = key;
    }

    public MetaData(String type, String key, String value) {
        mType = type;
        mKey = key;
        mValue = value;
    }

    public MetaData clone() {
        return new MetaData(this.mType, this.mKey, this.mValue);
    }

    public MetaData setValue(String value) {
        mValue = value;
        return this;
    }

    public String getKey() {
        return mKey;
    }

    public String getType() {
        return mType;
    }

    public String getValue() {
        return mValue;
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("metadata", indent).writeAttribute("type", mType);
        writer.writeAttribute("key", mKey).writeData(mValue).endElement();

    }
}
