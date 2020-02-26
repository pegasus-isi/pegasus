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

import edu.isi.ikcap.workflows.util.logging.EventLogMessage;
import edu.isi.ikcap.workflows.util.logging.LogEvent;
import edu.isi.pegasus.common.logging.*;
import java.util.Collection;
import java.util.Map;

/**
 * The netlogger event.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class NetloggerEvent implements Event {

    /** The name of the program. */
    private String mProgram;

    /** The current event object. */
    private LogEvent mLogEvent;

    /** The current log event message. */
    private EventLogMessage mMessage;

    /** The default constructor. */
    public NetloggerEvent() {}

    /**
     * Sets the program name for the software whose log statement are logged.
     *
     * @param name
     */
    public void setProgramName(String name) {
        mProgram = name;
    }

    /**
     * Returns the program name for the software whose log statement are logged.
     *
     * @param name
     * @return the name of the program
     */
    public String getProgramName(String name) {
        return mProgram;
    }

    /**
     * Set the event that is to be associated with the log messages.
     *
     * @param name the name of the event to be associated
     * @param entityName the primary entity that is associated with the event e.g. workflow
     * @param entityID the id of that entity.
     */
    public void setEvent(String name, String entityName, String entityID) {
        mLogEvent = new LogEvent(name, mProgram, entityName, entityID);
        reset();
    }

    /**
     * Adds the event that is to be associated with the log messages onto an internal stack
     *
     * @param name the name of the event to be associated
     * @param map Map of Entity Names with the entity identifiers.
     */
    public void setEvent(String name, Map<String, String> map) {
        mLogEvent = new LogEvent(name, mProgram, map);
        reset();
    }

    /**
     * Returns the name of event that is currently associated with the log messages.
     *
     * @return the event to be associated
     */
    public String getEventName() {
        return null;
    }

    /**
     * Creates the start message for the event.
     *
     * @return start event message
     */
    public String getStartEventMessage() {
        return mLogEvent.createStartLogMsg().toString();
    }

    /**
     * Creates the end message for the event.
     *
     * @return end event message
     */
    public String getEndEventMessage() {
        return mLogEvent.createEndLogMsg().toString();
    }

    /** Reset the internal log message buffer associated with the event */
    public void reset() {
        mMessage = mLogEvent.createLogMsg();
    }

    /**
     * Add to the log message.
     *
     * @param key
     * @param value
     * @return Self-reference, so calls can be chained
     */
    public Event add(String key, String value) {
        mMessage = mMessage.addWQ(key, value);
        return this;
    }

    /**
     * Creates a log message with the contents of the internal log buffer.
     *
     * @return the log message
     */
    public String createLogMessage() {
        return mMessage.toString();
    }

    /**
     * Creates a log message with the contents of the internal log buffer. It then resets the buffer
     * before returning the log message
     *
     * @return the log message
     */
    public String createLogMessageAndReset() {
        String result = mMessage.toString();
        this.reset();
        return result;
    }

    /**
     * Creates a log message that connects the parent entities with the children. For e.g. can we
     * use to create the log messages connecting the jobs with the workflow they are part of.
     *
     * @param parentType the type of parent entity
     * @param parentID the id of the parent entity
     * @param childIdType the type of children entities
     * @param childIDs Collection of children id's
     * @return entity hierarch message.
     */
    public String createEntityHierarchyMessage(
            String parentType, String parentID, String childIdType, Collection<String> childIDs) {
        return LogEvent.createIdHierarchyLogMsg(
                        parentType, parentID, childIdType, childIDs.iterator())
                .toString();
    }
}
