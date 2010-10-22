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
public class Parent {

    /**
     * The name of the parent
     */
    private String mName;
    /**
     * The edge label for the parent child relationship. Optional.
     */
    private String mLabel;

    /**
     *
     * @param name
     */
    public Parent(String name) {
        mName = name;
    }

    public Parent(Parent p) {
        this(p.getName(), p.getLabel());
    }

    /**
     *
     * @param name
     * @param label
     */
    public Parent(String name, String label) {
        mName = name;
        mLabel = label;
    }

    /**
     * @return the name of the parent
     */
    public String getName() {
        return mName;
    }

    /**
     * @param name the name of the parent to set
     */
    public void setName(String name) {
        mName = name;
    }

    /**
     * @return the label
     */
    public String getLabel() {
        return mLabel;
    }

    /**
     * @param label the label to set
     */
    public void setLabel(String label) {
        mLabel = label;
    }

    public Parent clone() {
        return new Parent(this.mName, this.mLabel);
    }

    @Override
    public int hashCode() {
        int hashcode;
        if (mLabel == null) {
            hashcode = 0;
        } else {
            hashcode = mLabel.hashCode();
        }
        return 31 * mName.hashCode() + hashcode;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Parent)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        return mName.equals(((Parent) o).getName()) && mLabel.equals(((Parent) o).getLabel());
    }

    @Override
    public String toString() {
        return "(" + mName + ", " + mLabel == null ? "" : mLabel + ')';
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("parent", indent);
        writer.writeAttribute("ref", mName);
        if (mLabel != null && !mLabel.isEmpty()) {
            writer.writeAttribute("edge-label", mLabel);
        }
        writer.endElement();

    }
}
