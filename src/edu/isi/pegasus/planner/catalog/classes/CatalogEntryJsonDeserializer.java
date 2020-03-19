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
package edu.isi.pegasus.planner.catalog.classes;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import edu.isi.pegasus.planner.catalog.CatalogException;

/**
 * Abstract Class for Deserializers for parsing Catalog Data Objects
 *
 * @author Karan Vahi
 */
public abstract class CatalogEntryJsonDeserializer<T> extends JsonDeserializer<T> {
    /**
     * Throw an exception for Illegal Key
     *
     * @param element
     * @param node
     * @param key
     * @throws SiteCatalogException
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
     * @throws SiteCatalogException
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
     * @throws SiteCatalogException
     */
    public void complain(String prefix, String element, String key, JsonNode node)
            throws CatalogException {
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
        throw new CatalogException(sb.toString());
    }
}
