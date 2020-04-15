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
package edu.isi.pegasus.planner.classes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import edu.isi.pegasus.planner.common.PegasusJsonDeserializer;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A container class that stores all the notifications that need to be done indexed by the various
 * conditions.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
@JsonDeserialize(using = Notifications.JsonDeserializer.class)
public class Notifications extends Data {

    /**
     * An enum map that associates the various notification events with the list of actions that
     * need to be taken.
     */
    private EnumMap<Invoke.WHEN, List<Invoke>> mInvokeMap;

    /** The default constructor. */
    public Notifications() {
        reset();
    }

    /** Resets the internal invoke map. */
    public void reset() {
        mInvokeMap = new EnumMap<Invoke.WHEN, List<Invoke>>(Invoke.WHEN.class);

        Invoke.WHEN[] values = Invoke.WHEN.values();
        for (int i = 0; i < values.length; i++) {
            mInvokeMap.put(values[i], new LinkedList());
        }
    }

    /**
     * Adds a Invoke object correpsonding to a notification.
     *
     * @param notification the notification object
     */
    public void add(Invoke notification) {

        if (notification == null) {
            return; // do nothing
        }
        // retrieve the appropriate namespace and then add
        List<Invoke> l = (List) mInvokeMap.get(Invoke.WHEN.valueOf(notification.getWhen()));
        l.add(notification);
    }

    /**
     * Adds all the notifications passed to the underlying container.
     *
     * @param notifications the notification object
     */
    public void addAll(Notifications notifications) {

        if (notifications == null) {
            return; // do nothing
        }
        for (Invoke.WHEN when : Invoke.WHEN.values()) {
            this.addAll(when, notifications.getNotifications(when));
        }
    }

    /**
     * Returns a collection of all the notifications that need to be done for a particular condition
     *
     * @param when the condition
     * @return
     */
    public Collection<Invoke> getNotifications(String when) {
        return this.getNotifications(Invoke.WHEN.valueOf(when));
    }

    /**
     * Returns a collection of all the notifications that need to be done for a particular condition
     *
     * @param when the condition
     * @return
     */
    public Collection<Invoke> getNotifications(Invoke.WHEN when) {
        return this.mInvokeMap.get(when);
    }

    /**
     * Returns a boolean indicating whether the notifications object is empty or not.
     *
     * @return true if empty else false
     */
    public boolean isEmpty() {
        Invoke.WHEN[] values = Invoke.WHEN.values();
        for (int i = 0; i < values.length; i++) {
            if (!mInvokeMap.get(values[i]).isEmpty()) return false;
        }
        return true;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Notifications inv;
        try {
            inv = (Notifications) super.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }

        // traverse through all the enum keys
        for (Invoke.WHEN when : Invoke.WHEN.values()) {
            Collection<Invoke> c = this.getNotifications(when);
            inv.addAll(when, c);
        }
        return inv;
    }

    /**
     * Returns a String description of the object
     *
     * @return
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Invoke.WHEN when : Invoke.WHEN.values()) {
            Collection<Invoke> c = this.getNotifications(when);
            for (Invoke invoke : c) {
                sb.append(invoke.toString());
            }
        }
        return sb.toString();
    }

    /**
     * Convenience method at add all the notifications corresponding to a particular event
     *
     * @param when when does the event happen
     * @param notifications the list of notificiations
     */
    private void addAll(WHEN when, Collection<Invoke> invokes) {
        Collection<Invoke> c = this.mInvokeMap.get(when);
        c.addAll(invokes);
    }

    /**
     * Returns whether a particular notification exists or not
     *
     * @param invoke
     * @return
     */
    boolean contains(Invoke invoke) {
        Collection<Invoke> c = this.getNotifications(invoke.getWhen());
        return c == null ? false : c.contains(invoke);
    }

    /**
     * Custom deserializer for YAML representation of Notifications/hooks
     *
     * @author Karan Vahi
     */
    static class JsonDeserializer extends PegasusJsonDeserializer<Notifications> {

        public JsonDeserializer() {}

        /**
         * Deserializes a Transformation YAML description of the type
         *
         * <pre>
         *    shell:
         *      - _on: start
         *        cmd: /bin/date
         *      - _on: end
         *        cmd: /bin/echo "Finished"
         * </pre>
         *
         * @param parser
         * @param dc
         * @return
         * @throws IOException
         * @throws JsonProcessingException
         */
        @Override
        public Notifications deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);
            Notifications notifications = new Notifications();
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                WorkflowKeywords reservedKey = WorkflowKeywords.getReservedKey(key);
                if (reservedKey == null) {
                    this.complainForIllegalKey(WorkflowKeywords.HOOKS.getReservedName(), key, node);
                }
                notifications.addAll(this.createNotifications(key, node.get(key)));
            }
            return notifications;
        }

        /**
         * Parses an array of notifications of same type
         *
         * <pre>
         *      - _on: start
         *        cmd: /bin/date
         *      - _on: end
         *        cmd: /bin/echo "Finished"
         * </pre>
         *
         * @param type
         * @param node
         * @return
         */
        protected Notifications createNotifications(String type, JsonNode node) {
            Notifications notifications = new Notifications();
            if (type.equals("shell")) {
                if (node.isArray()) {
                    for (JsonNode hook : node) {
                        notifications.add(
                                new Invoke(
                                        Invoke.WHEN.valueOf(hook.get("_on").asText()),
                                        hook.get("cmd").asText()));
                    }
                } else {
                    throw new RuntimeException("Expected an array of shell hooks " + node);
                }
            } else {
                throw new RuntimeException(
                        "Unsupported notifications of type " + type + " - " + node);
            }
            return notifications;
        }

        @Override
        public RuntimeException getException(String message) {
            return new RuntimeException(message);
        }
    }
}
