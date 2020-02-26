/**
 * Copyright 2007-2012 University Of Southern California
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
 * Creates a DAX job object
 *
 * @author GAURANG MEHTA gmehta at isi dot edu
 * @see AbstractJob
 * @version $Revision$
 */
public class DAX extends AbstractJob {

    /**
     * Create a DAX job object
     *
     * @param id The unique id of the DAX job object. Must be of type [A-Za-z][-A-Za-z0-9_]*
     * @param dagname The DAX file to plan and submit
     */
    public DAX(String id, String daxname) {
        this(id, daxname, null);
    }

    /**
     * Copy Constructor
     *
     * @param dax
     */
    public DAX(DAX dax) {
        super(dax);
    }

    /**
     * Create a DAX job object
     *
     * @param id The unique id of the DAX job object. Must be of type [A-Za-z][-A-Za-z0-9_]*
     * @param dagname The DAX file to plan and submit
     * @param label
     */
    public DAX(String id, String daxname, String label) {
        super();
        checkID(id);
        // to decide whether to exit. Currently just logging error and proceeding.
        mId = id;
        mName = daxname;
        mNodeLabel = label;
    }

    /**
     * Is this Object a DAX
     *
     * @return
     */
    public boolean isDAX() {
        return true;
    }

    /**
     * @param writer
     * @param indent
     */
    public void toXML(XMLWriter writer, int indent) {

        writer.startElement("dax", indent);
        writer.writeAttribute("id", mId);
        writer.writeAttribute("file", mName);
        super.toXML(writer, indent);
    }
}
