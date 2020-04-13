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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.WorkflowKeywords;
import edu.isi.pegasus.planner.common.PegasusJsonDeserializer;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import edu.isi.pegasus.planner.dax.Invoke;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** @author Karan Vahi */
@JsonDeserialize(using = DAXParser5.YAMLStreamingDeserializer.class)
public class DAXParser5 implements DAXParser {

    /** Handle to the callback */
    protected Callback mCallback;

    /** Bag of initialization objects */
    private final PegasusBag mBag;

    /**
     * The overloaded constructor. The schema version passed is determined in the DAXFactory
     *
     * @param bag
     * @param schemaVersion the schema version specified in the DAX file.
     */
    public DAXParser5(PegasusBag bag, String schemaVersion) {
        mBag = bag;
    }

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
    public void parse(String file) {
        Reader reader;
        try {
            reader = new VariableExpansionReader(new FileReader(file));
        } catch (IOException ioe) {
            throw new RuntimeException("Exception while reading file " + file);
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        mapper.setInjectableValues(injectCallback());
        try {
            mapper.readValue(reader, DAXParser5.class);
        } catch (IOException ex) {
            throw new RuntimeException("Exception while parsing yaml file " + file);
        }
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
    static class YAMLStreamingDeserializer extends PegasusJsonDeserializer<DAXParser5> {

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

                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            WorkflowKeywords.WORKFLOW.getReservedName(), key, node);
                }
                Map attrs = new HashMap();
                attrs.put("index", "0");
                switch (reservedKey) {
                    case PEGASUS:
                        attrs.put("version", node.get(key).asText());
                        break;

                    case NAME:
                        attrs.put("name", node.get(key).asText());
                        c.cbDocument(attrs);
                        break;

                    case REPLICA_CATALOG:
                        JsonNode rcNode = node.get(key);
                        if (rcNode != null) {
                            parser = rcNode.traverse(oc);
                            ReplicaStore store = parser.readValueAs(ReplicaStore.class);
                            c.cbReplicaStore(store);
                        }
                        break;

                    case SITE_CATALOG:
                        JsonNode scNode = node.get(key);
                        if (scNode != null) {
                            parser = scNode.traverse(oc);
                            SiteStore store = parser.readValueAs(SiteStore.class);
                            c.cbSiteStore(store);
                        }
                        break;

                    case TRANSFORMATION_CATALOG:
                        JsonNode tcNode = node.get(key);
                        if (tcNode != null) {
                            parser = tcNode.traverse(oc);
                            TransformationStore store =
                                    parser.readValueAs(TransformationStore.class);
                            c.cbTransformationStore(store);
                        }
                        break;

                    case HOOKS:
                        JsonNode hooksNode = node.get(key);
                        Notifications notifications = this.createNotifications(hooksNode);
                        for (Invoke.WHEN when : Invoke.WHEN.values()) {
                            for (Invoke i : notifications.getNotifications(when)) {
                                c.cbWfInvoke(i);
                            }
                        }
                        break;

                    case JOBS:
                        JsonNode jobsNode = node.get(key);
                        if (jobsNode.isArray()) {
                            for (JsonNode jobNode : jobsNode) {
                                parser = jobNode.traverse(oc);
                                Job job = parser.readValueAs(Job.class);
                                c.cbJob(job);
                            }
                        } else {
                            throw new RuntimeException("jobs: value should be of type array ");
                        }
                        break;

                    case JOB_DEPENDENCIES:
                        JsonNode dependenciesNode = node.get(key);
                        if (dependenciesNode.isArray()) {
                            for (JsonNode dependencyNode : dependenciesNode) {
                                String jobID =
                                        dependencyNode
                                                .get(WorkflowKeywords.JOB_ID.getReservedName())
                                                .asText();
                                List<String> children =
                                        this.createChildren(
                                                dependencyNode.get(
                                                        WorkflowKeywords.CHILDREN
                                                                .getReservedName()));
                                c.cbChildren(jobID, children);
                            }
                        } else {
                            throw new RuntimeException(
                                    WorkflowKeywords.JOB_DEPENDENCIES
                                            + ": value should be of type array ");
                        }
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                WorkflowKeywords.WORKFLOW.getReservedName(), key, node);
                }
            }
            c.cbDone();
            return null;
        }

        @Override
        public RuntimeException getException(String message) {
            return new RuntimeException(message);
        }

        /**
         * Returns a list of children id
         *
         * @param node
         * @return
         */
        private List<String> createChildren(JsonNode node) {
            List<String> ids = new LinkedList();
            if (node.isArray()) {
                for (JsonNode idNode : node) {
                    ids.add(idNode.asText());
                }
            } else {
                throw new RuntimeException(
                        WorkflowKeywords.CHILDREN + ": value should be of type array ");
            }
            return ids;
        }
    }

    public static void main(String[] args) {

        Callback c = new DAX2CDAG();
        String dax = "/Users/vahi/Pegasus/work/yaml-tc/workflow.yml";
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManager.getInstance("", ""));
        DAXParser5 parser = new DAXParser5(bag, "5.0");
        c.initialize(bag, dax);
        parser.setDAXCallback(c);
        parser.parse(dax);
    }
}
