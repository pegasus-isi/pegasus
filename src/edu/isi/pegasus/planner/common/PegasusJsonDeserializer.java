/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.common;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.namespace.Namespace;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Abstract Class for Deserializers for parsing YAML objects with convenient
 * helper methods
 *
 * @author Karan Vahi
 * @param <T>
 */
public abstract class PegasusJsonDeserializer<T> extends JsonDeserializer<T> {

    /**
     * Throw an exception for Illegal Key
     *
     * @param element
     * @param node
     * @param key
     */
    public void complainForIllegalKey(String element, String key, JsonNode node)
            throws CatalogException {
        this.complain("Illegal key", element, key, node);
    }

    /**
     * Throw an exception for Illegal Key
     *
     * @param prefix
     * @param element
     * @param key
     * @param node
     */
    public void complainForUnsupportedKey(String element, String key, JsonNode node)
            throws CatalogException {
        this.complain("Unsupported key", element, key, node);
    }

    /**
     * Throw an exception
     *
     * @param prefix
     * @param element
     * @param key
     * @param node
     */
    public void complain(String prefix, String element, String key, JsonNode node)
            throws RuntimeException {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix)
                .append(" ")
                .append(key)
                .append(" ")
                .append("for element")
                .append(" ")
                .append(element)
                .append(" - ")
                .append(node.toString());
        throw getException(sb.toString());
    }
    
    /**
     * Creates a metadata key value pairs as profiles
     *
     * <pre>
     * APP_HOME: "/tmp/myscratch"
     * JAVA_HOME: "/opt/java/1.6"
     * </pre>
     *
     * @param namespace
     * @param node
     * @return Profiles
     */
    protected List<Profile> createMetadata(JsonNode node) {
        return this.createProfiles("metadata", node);
    }
    
    /**
     * Creates a profile from a JSON node representing
     *
     * <pre>
     * APP_HOME: "/tmp/myscratch"
     * JAVA_HOME: "/opt/java/1.6"
     * </pre>
     *
     * @param namespace
     * @param node
     * @return Profiles
     */
    protected List<Profile> createProfiles(String namespace, JsonNode node) {
        List<Profile> profiles = new LinkedList();
        if (Namespace.isNamespaceValid(namespace)) {
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                profiles.add(new Profile(namespace, entry.getKey(), entry.getValue().asText()));
            }
        } else {
            throw new CatalogException(
                    "Invalid namespace specified " + namespace + " for profiles " + node);
        }
        return profiles;
    }
    
    /**
     * Creates a notifications object
     *
     * <pre>
     *   shell:
     *        - on: start
     *          cmd: /bin/date
     * </pre>
     *
     * @param node
     * @return
     */
    protected Notifications createNotifications(JsonNode node) {
        Notifications n = new Notifications();
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            n.addAll(this.createNotifications(entry.getKey(), entry.getValue()));
        }
        return n;
    }

    /**
     * Parses an array of notifications of same type
     *
     * <p>- on: start cmd: /bin/date
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
                throw new CatalogException("Expected an array of hooks " + node);
            }
        } else {
            throw new CatalogException("Unsupported notifications of type " + type + " - " + node);
        }
        return notifications;
    }
    
    /**
     * The exception to be thrown while deserializing on error
     *
     * @param message the error message
     *
     * @return
     */
    public abstract RuntimeException getException(String message);
}
