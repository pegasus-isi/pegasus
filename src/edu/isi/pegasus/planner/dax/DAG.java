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
 * DAG Class to hold the DAG job object.
 *
 * @see AbstractJob
 * @author Gaurang Mehta gmehta at isi dot edu
 * @version $Revision$
 */
public class DAG extends AbstractJob {

    /**
     * Create a DAG object
     *
     * @param id The unique id of the DAG job object. Must be of type [A-Za-z][-A-Za-z0-9_]*
     * @param dagname The dag file to submit
     */
    public DAG(String id, String dagname) {
        this(id, dagname, null);
    }

    /**
     * Copy Constructor
     *
     * @param dag
     */
    public DAG(DAG dag) {
        super(dag);
    }

    /**
     * Create a DAG object
     *
     * @param id The unique id of the DAG job object. Must be of type [A-Za-z][-A-Za-z0-9_]*
     * @param dagname The dag file to submit
     * @param label The label for this job.
     */
    public DAG(String id, String dagname, String label) {
        super();
        checkID(id);
        // to decide whether to exit. Currently just logging error and proceeding.
        mId = id;
        mName = dagname;
        mNodeLabel = label;
    }

    /**
     * Is this Object a DAG
     *
     * @return
     */
    public boolean isDAG() {
        return true;
    }

    public void toXML(XMLWriter writer, int indent) {

        writer.startElement("dag", indent);
        writer.writeAttribute("id", mId);
        writer.writeAttribute("file", mName);
        super.toXML(writer, indent);
    }
}
