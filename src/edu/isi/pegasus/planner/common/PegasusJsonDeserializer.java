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
     * The exception to be thrown while deserializing on error
     *
     * @param message the error message
     *
     * @return
     */
    public abstract RuntimeException getException(String message);
}
