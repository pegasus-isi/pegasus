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
package edu.isi.pegasus.common.logging;

import java.util.Collection;
import java.util.Map;

/**
 * The interface that defines how the messages need to be formatted for logging
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public interface LogFormatter {

    /**
     * Sets the program name for the software whose log statement are logged.
     *
     * @param name
     */
    public void setProgramName(String name);

    /**
     * Returns the program name for the software whose log statement are logged.
     *
     * @param name
     * @return name of the program
     */
    public String getProgramName(String name);

    /**
     * Adds the event that is to be associated with the log messages onto an internal stack
     *
     * @param name the name of the event to be associated
     * @param entityName the primary entity that is associated with the event e.g. workflow
     * @param entityID the id of that entity.
     */
    public void addEvent(String name, String entityName, String entityID);

    /**
     * Adds the event that is to be associated with the log messages onto an internal stack
     *
     * @param name the name of the event to be associated
     * @param map Map indexed by entity name . The values is corresponding EntityID
     */
    public void addEvent(String name, Map<String, String> map);

    /**
     * Pop the event on top of the internal stack.
     *
     * @return event on top , else null
     */
    public Event popEvent();

    /**
     * Returns the name of event that is currently associated with the log messages and is on the
     * top of the stack
     *
     * @return name of the event.
     */
    public String getEventName();

    /**
     * Creates the start message for the event on top of the internal stack
     *
     * @return start event message
     */
    public String getStartEventMessage();

    /**
     * Creates the end message for the event on top of the stack.
     *
     * @return end event message
     */
    public String getEndEventMessage();

    /**
     * Add to the log message with just a value.
     *
     * @param value
     * @return self-reference
     */
    public LogFormatter add(String value);

    /**
     * Add to the log message.
     *
     * @param key
     * @param value
     * @return Self-reference, so calls can be chained
     */
    public LogFormatter add(String key, String value);

    /**
     * Creates a log message with the contents of the internal log buffer.
     *
     * @return log message
     */
    public String createLogMessage();

    /**
     * Creates a log message with the contents of the internal log buffer. It then resets the buffer
     * before returning the log message
     *
     * @return the log message
     */
    public String createLogMessageAndReset();

    /**
     * Creates a log message that connects the parent entities with the children. For e.g. can we
     * use to create the log messages connecting the jobs with the workflow they are part of.
     *
     * @param parentType the type of parent entity
     * @param parentID the id of the parent entity
     * @param childIdType the type of children entities
     * @param childIDs Collection of children id's
     * @return entity hierarchy message.
     */
    public String createEntityHierarchyMessage(
            String parentType, String parentID, String childIdType, Collection<String> childIDs);
}
