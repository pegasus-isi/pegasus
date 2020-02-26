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
 * @author gmehta
 * @version $Revision: 3009 $
 */
public class Edge {

    /** The parent of the child */
    private String mChild;
    /** The parent of the parent */
    private String mParent;
    /** The edge label for the parent child relationship. Optional. */
    private String mLabel;

    /** @param parent */
    public Edge(String parent, String child) {
        mParent = parent;
        mChild = child;
    }

    public Edge(Edge e) {
        this(e.getParent(), e.getChild(), e.getLabel());
    }

    /**
     * @param parent
     * @param label
     */
    public Edge(String parent, String child, String label) {
        mParent = parent;
        mChild = child;
        mLabel = label;
    }

    /** @return the parent */
    public String getParent() {
        return mParent;
    }

    /** @param parent the parent of the edge to set */
    public void setParent(String parent) {
        mParent = parent;
    }

    /** @return the child of the edge */
    public String getChild() {
        return mChild;
    }

    /** @param parent the child of the edge to set */
    public void setChild(String child) {
        mChild = child;
    }

    /** @return the label */
    public String getLabel() {
        return mLabel;
    }

    /** @param label the label to set */
    public void setLabel(String label) {
        mLabel = label;
    }

    public Edge clone() {
        return new Edge(this.mParent, this.mChild, this.mLabel);
    }

    @Override
    public int hashCode() {
        return 31 * mParent.hashCode()
                + 12 * mChild.hashCode()
                + (mLabel == null ? 0 : 29 * mLabel.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Edge)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        return mParent.equals(((Edge) o).getParent())
                && mChild.equals(((Edge) o).getChild())
                && mLabel.equals(((Edge) o).getLabel());
    }

    @Override
    public String toString() {
        return "(" + mParent + "->" + (mChild + mLabel == null ? "" : ":" + mLabel) + ')';
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXMLParent(XMLWriter writer) {
        toXMLParent(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("child", indent);
        writer.startElement("parent", indent + 1);
        writer.writeAttribute("ref", mParent);
        if (mLabel != null && !mLabel.isEmpty()) {
            writer.writeAttribute("edge-label", mLabel);
        }
        writer.noLine();
        writer.endElement();
        writer.endElement(indent);
    }

    public void toXMLParent(XMLWriter writer, int indent) {
        writer.startElement("parent", indent);
        writer.writeAttribute("ref", mParent);
        if (mLabel != null && !mLabel.isEmpty()) {
            writer.writeAttribute("edge-label", mLabel);
        }
        writer.endElement();
    }
}
