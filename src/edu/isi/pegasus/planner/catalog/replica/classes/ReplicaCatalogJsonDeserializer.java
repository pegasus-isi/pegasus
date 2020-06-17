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
import edu.isi.pegasus.planner.namespace.Metadata;
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
     * - lfn: f1
     *   pfns:
     *     - site: local
     *       pfn: /path/to/file
     *     - site: condorpool
     *       pfn: /path/to/file
     *   checksum:
     *      sha256: abc123
     *   metadata:
     *     owner: vahi
     *     size: 1024
     * </pre>
     *
     * @param node the json node
     * @return ReplicaLocation
     */
    protected ReplicaLocation createReplicaLocation(JsonNode node) {

        String lfn = null;
        ReplicaLocation rl = new ReplicaLocation();
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

                case PFNS:
                    JsonNode pfnNodes = node.get(key);
                    if (pfnNodes != null) {
                        if (pfnNodes.isArray()) {
                            for (JsonNode pfnNode : pfnNodes) {
                                rl.addPFN(this.createPFN(pfnNode));
                            }
                        }
                    }
                    break;

                case REGEX:
                    rl.addMetadata(key, keyValue);
                    break;

                case CHECKSUM:
                    addChecksum(rl, node.get(key));
                    break;
                    
                case METADATA:
                    addMetadata(rl, node.get(key));
                    break;

                default:
                    this.complainForUnsupportedKey(
                            ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
            }
        }
        if (lfn == null) {
            throw new ReplicaCatalogException("Replica needs to be defined with a lfn " + node);
        }
        rl.setLFN(lfn);
        System.err.println(rl);
        return rl;
    }

    /**
     * Parses checksum information and adds it to the replica catalog entry object
     *
     * @param rl
     * @param node
     */
    private void addChecksum(ReplicaLocation rl, JsonNode node) {

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
                    case SHA256:
                        rl.addMetadata(Metadata.CHECKSUM_TYPE_KEY, "sha256");
                        rl.addMetadata(Metadata.CHECKSUM_VALUE_KEY, keyValue);
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                ReplicaCatalogKeywords.CHECKSUM.getReservedName(), key, node);
                }
            }
        } else {
            throw new RuntimeException(
                    "Checksum needs to be object node. Found for replica" + node);
        }
    }

    /**
     * Deserializes a pfn of type below
     * 
     * <pre>
     * - site: local
     *   pfn: /url/to/file
     * </pre>
     * 
     * @param node
     * @return 
     */
    private ReplicaCatalogEntry createPFN(JsonNode node) {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        if (node instanceof ObjectNode) {
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            ReplicaCatalogKeywords.PFNS.getReservedName(), key, node);
                }

                String keyValue = node.get(key).asText();
                switch (reservedKey) {
                    case PFN:
                        rce.setPFN(keyValue);
                        break;

                    case SITE:
                        rce.setResourceHandle(keyValue);
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                ReplicaCatalogKeywords.PFNS.getReservedName(), key, node);
                }
            }
        } else {
            throw new RuntimeException(
                    "PFN needs to be object node. Found for replica" + node);
        }
        
        return rce;
    }

    /**
     * Parses any metadata into the ReplicaLocation object
     *
     * @param rl
     * @param node
     */
    private void addMetadata(ReplicaLocation rl, JsonNode node) {
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> entry = it.next();
            rl.addMetadata(entry.getKey(), entry.getValue().asText());
        }
    }
}
