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
public class Invoke {

    public static enum WHEN {

        never, start, on_success, on_error, at_end, all
    };
    protected WHEN mWhen;
    protected String mWhat;

    public Invoke(Invoke i) {
        this(WHEN.valueOf(i.getWhen()), i.getWhat());
    }

    public Invoke(WHEN when) {
        mWhen = when;
    }

    public Invoke(WHEN when, String what) {
        mWhen = when;
        mWhat = what;

    }

    public String getWhen() {
        return mWhen.toString();
    }

    public Invoke setWhen(WHEN when) {
        mWhen = when;
        return this;
    }

    public String getWhat() {
        return mWhat;
    }

    public Invoke setWhat(String what) {
        mWhat = what;
        return this;
    }

    public Invoke clone() {
        return new Invoke(this.mWhen, this.mWhat);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("invoke", indent);
        writer.writeAttribute("when", mWhen.toString().toLowerCase());
        writer.writeData(mWhat).endElement();


    }
}
