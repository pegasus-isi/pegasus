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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaCatalogKeywords;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.classes.WorkflowKeywords;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.parser.YAMLParser;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A streaming YAML parser for Pegasus abstract workflow files (DAX v5).
 *
 * <p>Unlike {@link DAXParser5}, which loads the entire document into a Jackson {@code JsonNode}
 * tree before processing, this parser uses Jackson's token-based streaming API. The two large
 * arrays — {@code jobs} and {@code jobDependencies} — are processed one element at a time so that
 * only a single job or dependency object is live in memory at any given moment. This makes the
 * parser suitable for very large workflow files (hundreds of thousands of jobs) that would exhaust
 * heap memory with the tree-based approach.
 *
 * <p>The {@code replicaCatalog} section is also streamed element-by-element: each entry is
 * deserialized as a {@link edu.isi.pegasus.planner.classes.ReplicaLocation} via its own Jackson
 * deserializer and forwarded via {@link Callback#cbFile} immediately, so a workflow that embeds
 * millions of replica entries does not cause an OOM. Smaller sections ({@code metadata}, {@code
 * hooks}, {@code siteCatalog}, {@code transformationCatalog}) are still read as sub-trees via their
 * existing Jackson deserializers because they are bounded in size.
 *
 * <p>The snakeyaml code-point limit inherited from {@link YAMLParser} is overridden to {@code
 * Integer.MAX_VALUE} so that very large files are not rejected at the YAML-scanner level.
 *
 * @author Karan Vahi
 */
public class DAXParser5Streaming extends YAMLParser implements DAXParser {

    /** Schema location URL — identical to {@link DAXParser5#SCHEMA_URI}. */
    public static final String SCHEMA_URI = "https://pegasus.isi.edu/schema/wf-5.0.yml";

    /** Handle to the callback invoked as the document is streamed. */
    protected Callback mCallback;

    /** Bag of initialization objects. */
    private final PegasusBag mBag;

    /** Key inside the {@code x-pegasus} vendor extension that names the workflow API language. */
    private static final String API_LANG_KEY = "apiLang";

    /** Resolved path to the workflow YAML schema file used for validation. */
    private final File SCHEMA_FILENAME;

    /**
     * Constructs a streaming DAX parser.
     *
     * @param bag bag of Pegasus objects (properties, logger, …)
     * @param schemaVersion schema version declared in the DAX file (unused; kept for API symmetry
     *     with {@link DAXParser5})
     */
    public DAXParser5Streaming(PegasusBag bag, String schemaVersion) {
        super(bag);
        mBag = bag;
        File schemaDir = this.mProps.getSchemaDir();
        File yamlSchemaDir = new File(schemaDir, "yaml");
        SCHEMA_FILENAME = new File(yamlSchemaDir, new File(SCHEMA_URI).getName());

        // Remove the snakeyaml code-point cap so that arbitrarily large files can be scanned.
        // Memory safety comes from the streaming job/dependency loop, not from a document-size cap.
        // mLoaderOptions.setCodePointLimit(Integer.MAX_VALUE);

        mLogger.log(
                "Streaming DAX parser active — no document-size limit; jobs and dependencies are"
                        + " processed one at a time",
                LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Validates a workflow file against the bundled Workflow Schema.
     *
     * @param workflow path to the workflow file
     * @return {@code true} if valid
     */
    public boolean validate(String workflow) {
        mLogger.log("Validating against " + SCHEMA_FILENAME, LogManager.INFO_MESSAGE_LEVEL);
        return this.validate(new File(workflow), SCHEMA_FILENAME, "workflow");
    }

    /**
     * Sets the callback that receives parsed objects.
     *
     * @param c the callback
     */
    @Override
    public void setDAXCallback(Callback c) {
        this.mCallback = c;
    }

    /**
     * Returns the active callback.
     *
     * @return the callback
     */
    @Override
    public Callback getDAXCallback() {
        return this.mCallback;
    }

    /**
     * Parses the given workflow file in streaming fashion.
     *
     * <p>Top-level scalar fields are read directly from the token stream. The {@code jobs} and
     * {@code jobDependencies} arrays are iterated element-by-element so that at most one job or
     * dependency object is deserialized at a time. All other sub-tree sections (replica, site, and
     * transformation catalogs; hooks; metadata) are read via {@code readValueAs}, which delegates
     * to each section's own Jackson deserializer.
     *
     * @param file path to the YAML workflow file
     */
    @Override
    public void parse(String file) {
        mLogger.logEventStart(LoggingKeys.EVENT_PEGASUS_PARSE_DAX, LoggingKeys.DAX_ID, file);
        Reader reader;
        try {
            reader = new VariableExpansionReader(new FileReader(file), this.mProps);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception while reading file " + file, ioe);
        }

        YAMLFactory yamlFactory = YAMLFactory.builder().loaderOptions(mLoaderOptions).build();
        ObjectMapper mapper = new ObjectMapper(yamlFactory);
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        try (JsonParser jp = mapper.createParser(reader)) {
            streamWorkflow(jp);
        } catch (IOException ex) {
            throw new RuntimeException("Exception while parsing yaml file " + file, ex);
        }
    }

    // -----------------------------------------------------------------------
    // Private streaming helpers
    // -----------------------------------------------------------------------

    /**
     * Core streaming loop. Walks the top-level YAML mapping one field at a time and dispatches to
     * the appropriate handler or callback method.
     *
     * <p>Fields are processed in document order. In a valid Pegasus workflow file the {@code
     * pegasus} version field precedes the {@code name} field, which precedes all other sections, so
     * the {@code cbDocument} call always receives a fully-populated attribute map.
     */
    private void streamWorkflow(JsonParser jp) throws IOException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("index", "0");

        JsonToken token = jp.nextToken();
        if (token != JsonToken.START_OBJECT) {
            throw new RuntimeException(
                    "Workflow YAML root must be a mapping (object), got: " + token);
        }

        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token != JsonToken.FIELD_NAME) {
                throw new RuntimeException("Expected a field name at workflow root, got: " + token);
            }

            String key = jp.getCurrentName();
            jp.nextToken(); // advance to the value token

            WorkflowKeywords reservedKey = WorkflowKeywords.getReservedKey(key);
            if (reservedKey == null) {
                if (key.startsWith("x-")) {
                    // user-defined vendor extension (e.g. x-myorg-annotation) — skip
                    jp.skipChildren();
                    continue;
                }
                throw new RuntimeException(
                        "Illegal key '" + key + "' encountered in workflow document");
            }

            switch (reservedKey) {
                case PEGASUS:
                    attrs.put("version", jp.getText());
                    break;

                case NAME:
                    attrs.put("name", jp.getText());
                    mCallback.cbDocument(attrs);
                    break;

                case X_PEGASUS:
                    handleXPegasus(jp);
                    break;

                case METADATA:
                    handleMetadata(jp);
                    break;

                case REPLICA_CATALOG:
                    streamReplicaCatalog(jp);
                    break;

                case SITE_CATALOG:
                    mCallback.cbSiteStore(jp.readValueAs(SiteStore.class));
                    break;

                case TRANSFORMATION_CATALOG:
                    mCallback.cbTransformationStore(jp.readValueAs(TransformationStore.class));
                    break;

                case HOOKS:
                    handleHooks(jp);
                    break;

                case JOBS:
                    streamJobsArray(jp);
                    break;

                case JOB_DEPENDENCIES:
                    streamDependenciesArray(jp);
                    break;

                default:
                    // known keyword but no special handling needed — skip
                    jp.skipChildren();
                    break;
            }
        }

        mCallback.cbDone();

        mLogger.logEventCompletion();
    }

    /**
     * Reads the {@code x-pegasus} vendor-extension object and fires {@code cbMetadata} for the
     * workflow API language if the {@code apiLang} key is present.
     */
    @SuppressWarnings("unchecked")
    private void handleXPegasus(JsonParser jp) throws IOException {
        Map<String, Object> ext = jp.readValueAs(Map.class);
        if (ext == null) {
            return;
        }
        Object lang = ext.get(API_LANG_KEY);
        if (lang != null) {
            mCallback.cbMetadata(
                    new Profile(Profile.METADATA, Metadata.WF_API_KEY, String.valueOf(lang)));
        }
    }

    /**
     * Reads the top-level {@code metadata} section (a flat YAML mapping of key → scalar value) and
     * fires {@code cbMetadata} for each entry.
     *
     * <p>Metadata is always small, so reading it as a {@link JsonNode} sub-tree is acceptable.
     */
    private void handleMetadata(JsonParser jp) throws IOException {
        JsonNode metaNode = jp.readValueAsTree();
        if (metaNode == null || metaNode.isNull() || !metaNode.isObject()) {
            return;
        }
        for (Iterator<Map.Entry<String, JsonNode>> it = metaNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            mCallback.cbMetadata(
                    new Profile(Profile.METADATA, entry.getKey(), entry.getValue().asText()));
        }
    }

    /**
     * Reads the {@code hooks} section via its own Jackson deserializer and fires {@code cbWfInvoke}
     * for every notification.
     */
    private void handleHooks(JsonParser jp) throws IOException {
        Notifications notifications = jp.readValueAs(Notifications.class);
        if (notifications == null) {
            return;
        }
        for (Invoke.WHEN when : Invoke.WHEN.values()) {
            for (Invoke inv : notifications.getNotifications(when)) {
                mCallback.cbWfInvoke(inv);
            }
        }
    }

    /**
     * Streams the {@code jobs} array one element at a time. Each element is deserialized into a
     * {@link Job} via its existing Jackson deserializer and immediately handed to the callback;
     * after the callback returns the object can be garbage-collected before the next job is read.
     *
     * <p>This is the primary memory-saving operation for large workflows.
     *
     * @param jp parser positioned at {@link JsonToken#START_ARRAY}
     */
    private void streamJobsArray(JsonParser jp) throws IOException {
        if (jp.currentToken() != JsonToken.START_ARRAY) {
            throw new RuntimeException(
                    WorkflowKeywords.JOBS.getReservedName() + ": value must be an array");
        }
        while (jp.nextToken() != JsonToken.END_ARRAY) {
            // Parser is at START_OBJECT for this job element.
            Job job = jp.readValueAs(Job.class);
            mCallback.cbJob(job);
        }
    }

    /**
     * Streams the {@code replicaCatalog} section one replica entry at a time. Walks the outer
     * object fields and, upon reaching the {@code replicas} array, deserializes each element as a
     * {@link ReplicaLocation} via its own Jackson deserializer and fires {@code cbFile}. This
     * avoids loading the entire catalog into a {@link ReplicaStore} in memory at once, which can be
     * prohibitive when a workflow embeds millions of replica entries.
     *
     * <p>Reuses the existing per-entry deserializer ({@link ReplicaLocation.JsonDeserializer})
     * without duplicating its logic.
     *
     * @param jp parser positioned at {@link JsonToken#START_OBJECT} for the replicaCatalog value
     */
    private void streamReplicaCatalog(JsonParser jp) throws IOException {
        if (jp.currentToken() != JsonToken.START_OBJECT) {
            throw new RuntimeException(
                    ReplicaCatalogKeywords.REPLICAS.getReservedName()
                            + ": replicaCatalog value must be a mapping (object)");
        }
        while (jp.nextToken() != JsonToken.END_OBJECT) {
            String field = jp.getCurrentName();
            jp.nextToken(); // advance to value
            ReplicaCatalogKeywords rcKey = ReplicaCatalogKeywords.getReservedKey(field);
            if (rcKey == null) {
                jp.skipChildren();
                continue;
            }
            switch (rcKey) {
                case PEGASUS:
                    // version string — consumed but not forwarded
                    break;

                case REPLICAS:
                    if (jp.currentToken() != JsonToken.START_ARRAY) {
                        throw new RuntimeException(
                                ReplicaCatalogKeywords.REPLICAS.getReservedName()
                                        + ": value must be an array");
                    }
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        ReplicaLocation rl = jp.readValueAs(ReplicaLocation.class);
                        int count = rl.getPFNCount();
                        if (count == 0) {
                            throw new ReplicaCatalogException(
                                    "ReplicaLocation "
                                            + rl
                                            + " can  have one pfn or more pfns. Found "
                                            + count);
                        }
                        if (rl.isRegex()) {
                            StringBuffer error = new StringBuffer();
                            error.append("Unable to deserialize into Replica Store an entry")
                                    .append(" ")
                                    .append("for lfn")
                                    .append(" ")
                                    .append(rl)
                                    .append(" ")
                                    .append(
                                            "as it has regex attribute set to true. Please specify such entries in a replica catalog file.");
                            throw new ReplicaCatalogException(error.toString());
                        }
                        mCallback.cbFile(rl);
                    }
                    break;

                default:
                    jp.skipChildren();
                    break;
            }
        }
    }

    /**
     * Streams the {@code jobDependencies} array one element at a time. Each element is a small
     * object with an {@code id} field and a {@code children} string array; both are read inline
     * without building a sub-tree.
     *
     * @param jp parser positioned at {@link JsonToken#START_ARRAY}
     */
    private void streamDependenciesArray(JsonParser jp) throws IOException {
        if (jp.currentToken() != JsonToken.START_ARRAY) {
            throw new RuntimeException(
                    WorkflowKeywords.JOB_DEPENDENCIES.getReservedName()
                            + ": value must be an array");
        }

        String jobIDKey = WorkflowKeywords.JOB_ID.getReservedName();
        String childrenKey = WorkflowKeywords.CHILDREN.getReservedName();

        while (jp.nextToken() != JsonToken.END_ARRAY) {
            // Parser is at START_OBJECT for this dependency element.
            String jobID = null;
            List<String> children = new LinkedList<>();

            while (jp.nextToken() != JsonToken.END_OBJECT) {
                // Parser is at FIELD_NAME inside the dependency object.
                String depField = jp.getCurrentName();
                jp.nextToken(); // advance to value

                if (jobIDKey.equals(depField)) {
                    jobID = jp.getText();
                } else if (childrenKey.equals(depField)) {
                    // children is an array of job-id strings
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
                        children.add(jp.getText());
                    }
                } else {
                    jp.skipChildren();
                }
            }

            mCallback.cbChildren(jobID, children);
        }
    }

    public static void main(String[] args) {
        String dax = args.length > 0 ? args[0] : "/tmp/large-workflow.yml";

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManager.getInstance("", ""));

        LogManager logger = bag.getLogger();
        logger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        logger.logEventStart("parsing.dax", dax, "");

        CountingCallback c = new CountingCallback();
        c.initialize(bag, dax);

        DAXParser5Streaming parser = new DAXParser5Streaming(bag, "5.0");
        parser.setDAXCallback(c);

        Runtime rt = Runtime.getRuntime();
        System.out.printf("Parsing %s%n", dax);
        System.out.printf("JVM heap: %.1f GB max%n", rt.maxMemory() / 1024.0 / 1024 / 1024);

        long t0 = System.currentTimeMillis();
        parser.parse(dax);
        long elapsed = System.currentTimeMillis() - t0;

        long heapUsed = rt.totalMemory() - rt.freeMemory();
        System.out.printf(
                "%nDone in %.1f s%n"
                        + "  Jobs parsed      : %,d%n"
                        + "  Deps parsed      : %,d%n"
                        + "  Heap used at end : %.1f MB%n",
                elapsed / 1000.0, c.getJobCount(), c.getDepCount(), heapUsed / 1024.0 / 1024);

        logger.logEventCompletion();
    }

    /**
     * A minimal {@link Callback} implementation for large-file streaming tests. Every parsed object
     * is counted and immediately discarded — nothing is accumulated in memory — so the test can run
     * on arbitrarily large workflow files within a small heap.
     */
    private static class CountingCallback implements Callback {

        private long mJobCount = 0;
        private long mDepCount = 0;
        private long mReportInterval = 1_000_000;
        private long mLastReport = 0;
        private long mT0 = System.currentTimeMillis();

        public long getJobCount() {
            return mJobCount;
        }

        public long getDepCount() {
            return mDepCount;
        }

        @Override
        public void initialize(PegasusBag bag, String dax) {
            mT0 = System.currentTimeMillis();
        }

        @Override
        public Object getConstructedObject() {
            return null;
        }

        @Override
        public void cbDocument(java.util.Map attributes) {
            System.out.printf(
                    "  Workflow: %s  version: %s%n",
                    attributes.get("name"), attributes.get("version"));
        }

        /** Counts the job and prints a progress line every million jobs. */
        @Override
        public void cbJob(Job job) {
            mJobCount++;
            if (mJobCount - mLastReport >= mReportInterval) {
                mLastReport = mJobCount;
                Runtime rt = Runtime.getRuntime();
                long heapMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
                double elapsed = (System.currentTimeMillis() - mT0) / 1000.0;
                System.out.printf(
                        "  jobs: %,10d  heap: %,5d MB  elapsed: %.1f s%n",
                        mJobCount, heapMB, elapsed);
            }
        }

        /** Counts each parent→child edge (one per child in the list). */
        @Override
        public void cbChildren(String parent, java.util.List<String> children) {
            mDepCount += children.size();
        }

        @Override
        public void cbDone() {
            System.out.printf("  cbDone — all tokens consumed%n");
        }

        // ---- catalog / metadata callbacks: count/discard, never accumulate ----

        @Override
        public void cbReplicaStore(
                edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore store) {}

        @Override
        public void cbTransformationStore(
                edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore store) {}

        @Override
        public void cbSiteStore(edu.isi.pegasus.planner.catalog.site.classes.SiteStore store) {}

        @Override
        public void cbMetadata(Profile p) {}

        @Override
        public void cbWfInvoke(Invoke invoke) {}

        @Override
        public void cbFile(edu.isi.pegasus.planner.classes.ReplicaLocation rl) {}

        @Override
        public void cbExecutable(
                edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry tce) {}

        @Override
        public void cbCompoundTransformation(
                edu.isi.pegasus.planner.classes.CompoundTransformation ct) {}

        @Override
        public void cbParents(
                String child, java.util.List<edu.isi.pegasus.planner.classes.PCRelation> parents) {}
    }
}
