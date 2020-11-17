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
package edu.isi.pegasus.common.logging.format;

import edu.isi.pegasus.common.logging.*;
import java.util.Map;

/**
 * This formatter formats the messages in the netlogger format.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Netlogger extends AbstractLogFormatter {

    /** The default constructor. */
    public Netlogger() {
        super();
    }

    /**
     * Adds the event that is to be associated with the log messages onto an internal stack
     *
     * @param name the name of the event to be associated
     * @param entityName the primary entity that is associated with the event e.g. workflow
     * @param entityID the id of that entity.
     */
    public void addEvent(String name, String entityName, String entityID) {
        Event e = new NetloggerEvent();
        e.setProgramName(mProgram);
        e.setEvent(name, entityName, entityID);
        mStack.addElement(e);
        return;
    }

    /**
     * Adds the event that is to be associated with the log messages onto an internal stack
     *
     * @param name the name of the event to be associated
     * @param map Map indexed by entity name . The values is corresponding EntityID
     */
    public void addEvent(String name, Map<String, String> map) {
        Event e = new NetloggerEvent();
        e.setProgramName(mProgram);
        e.setEvent(name, map);
        mStack.addElement(e);
        return;
    }
}
