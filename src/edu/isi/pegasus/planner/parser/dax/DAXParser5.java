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
package edu.isi.pegasus.planner.parser.dax;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.classes.WorkflowKeywords;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/** @author Karan Vahi */
@JsonDeserialize(using = DAXParser5.YAMLStreamingDeserializer.class)
public class DAXParser5 implements DAXParser {

    /** Handle to the callback */
    protected Callback mCallback;

    /**
     * Set the DAXCallback for the parser to call out to.
     *
     * @param c the callback
     */
    public void setDAXCallback(Callback c) {
        this.mCallback = c;
    }

    /**
     * Returns the DAXCallback for the parser
     *
     * @return the callback
     */
    public Callback getDAXCallback() {
        return this.mCallback;
    }

    /**
     * Start the parser. This starts the parsing of the file by the parser.
     *
     * @param file the path to the YAML file you want to parse.
     */
    public void startParser(String file) throws IOException {
        Reader reader = new VariableExpansionReader(new FileReader(file));
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        mapper.setInjectableValues(injectCallback());
        mapper.readValue(reader, DAXParser5.class);
    }

    /**
     * Set the Callback as an injectable value to insert into the Deserializer via Jackson.
     *
     * @return
     */
    private InjectableValues injectCallback() {
        return new InjectableValues.Std().addValue("callback", this.mCallback);
    }

    /**
     * Deserializer class that preserves the callback interface used for parsing the XML parsers.
     * Invokes callback functions during deserialization of the document.
     *
     * @author Karan Vahi
     */
    static class YAMLStreamingDeserializer extends JsonDeserializer<DAXParser5> {

        @Override
        public DAXParser5 deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            Callback c = (Callback) dc.findInjectableValue("callback", null, null);
            if (c == null) {
                throw new RuntimeException("Callback not initialized when parsing inititated");
            }
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);

            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                WorkflowKeywords reservedKey = WorkflowKeywords.getReservedKey(key);
                System.err.println(reservedKey);
                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            WorkflowKeywords.WORKFLOW.getReservedName(), key, node);
                }

                switch (reservedKey) {
                    case PEGASUS:
                        break;
                    case REPLICA_CATALOG:
                        JsonNode rcNode = node.get(key);
                        if (rcNode != null) {
                            parser = rcNode.traverse(oc);
                            ReplicaStore store = parser.readValueAs(ReplicaStore.class);
                            System.err.println(store);
                        }
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                WorkflowKeywords.WORKFLOW.getReservedName(), key, node);
                }
            }
            return null;
        }

        /**
         * Throw an exception for Illegal Key
         *
         * @param element
         * @param node
         * @param key
         * @throws RuntimeException
         */
        public void complainForIllegalKey(String element, String key, JsonNode node)
                throws RuntimeException {
            this.complain("Illegal key", element, key, node);
        }

        /**
         * Throw an exception for Illegal Key
         *
         * @param prefix
         * @param element
         * @param key
         * @param node
         * @throws RuntimeException
         */
        public void complainForUnsupportedKey(String element, String key, JsonNode node)
                throws RuntimeException {
            this.complain("Unsupported key", element, key, node);
        }

        /**
         * Throw an exception
         *
         * @param prefix
         * @param element
         * @param key
         * @param node
         * @throws RuntimeException
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
            throw new RuntimeException(sb.toString());
        }
    }

    public static void main(String[] args) {
        DAXParser5 parser = new DAXParser5();
        Callback c = new DAX2CDAG();
        parser.setDAXCallback(c);
        try {
            parser.startParser("/Users/vahi/Pegasus/work/yaml-tc/workflow.yml");
        } catch (IOException ex) {
            Logger.getLogger(DAXParser5.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
