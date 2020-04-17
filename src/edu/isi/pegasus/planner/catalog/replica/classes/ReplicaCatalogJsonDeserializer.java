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
package edu.isi.pegasus.planner.catalog.replica.classes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import java.util.Iterator;
import java.util.Map;

/**
 * Abstract Class for Deserializers for parsing Replica Catalog Data Objects
 *
 * @author Karan Vahi
 * @param <T>
 */
public abstract class ReplicaCatalogJsonDeserializer<T> extends CatalogEntryJsonDeserializer<T> {

    /**
     * The exception to be thrown while deserializing on error
     *
     * @param message the error message
     * @return
     */
    @Override
    public RuntimeException getException(String message) {
        return new CatalogException(message);
    }

    /**
     * Deserializes a Replica YAML description of the type
     *
     * <pre>
     *  # matches faa, f.a, f0a, etc.
     *  - lfn: "f.a"
     *    pfn: "file:///Volumes/data/input/f.a"
     *    site: "local"
     *    regex: true
     * </pre>
     *
     * @param node the json node
     * @return ReplicaLocation
     */
    protected ReplicaLocation createReplicaLocation(JsonNode node) {

        String lfn = null;
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(
                        ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
            }

            String keyValue = node.get(key).asText();
            switch (reservedKey) {
                case LFN:
                    lfn = keyValue;
                    break;

                case PFN:
                    rce.setPFN(keyValue);
                    break;

                case SITE:
                    rce.setResourceHandle(keyValue);
                    break;

                case REGEX:
                    rce.addAttribute(key, keyValue);
                    break;

                case CHECKSUM:
                    addChecksum(rce, node.get(key));
                    break;

                default:
                    this.complainForUnsupportedKey(
                            ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
            }
        }
        if (lfn == null) {
            throw new ReplicaCatalogException("Replica needs to be defined with a lfn " + rce);
        }
        if (rce.getPFN() == null) {
            throw new ReplicaCatalogException(
                    "Replica needs to be defined with a pfn for replica " + lfn + " " + rce);
        }
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN(lfn);
        rl.addPFN(rce);
        return rl;
    }

    /**
     * Parses checksum information and adds it to the replica catalog entry object
     * 
     * @param rce
     * @param node 
     */
    private void addChecksum(ReplicaCatalogEntry rce, JsonNode node) {

        if (node instanceof ObjectNode) {
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
                }

                String keyValue = node.get(key).asText();
                switch (reservedKey) {
                    case TYPE:
                        rce.addAttribute(key, keyValue);
                        break;

                    case VALUE:
                        rce.addAttribute(key, keyValue);
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
                }
            }
        } else {
            throw new RuntimeException(
                    "Checksum needs to be object node. Found for replica" + node);
        }
    }
}
