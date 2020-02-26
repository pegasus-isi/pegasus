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
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.classes.ReplicaStore;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

/**
 * A generator that writes out the replica store containing a file based replica catalog that has
 * the file locations mentioned in the DAX.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXReplicaStore implements CodeGenerator {

    /**
     * The name of the source key for Replica Catalog Implementer that serves as the repository for
     * DAX Replica Store
     */
    public static final String DAX_REPLICA_STORE_CATALOG_KEY = "file";

    /** The name of the Replica Catalog Implementer that serves as the source for cache files. */
    public static final String DAX_REPLICA_STORE_CATALOG_IMPLEMENTER = "SimpleFile";

    /** Suffix to be applied for cache file generation. */
    private static final String CACHE_FILE_SUFFIX = ".cache";

    /** Suffix to be applied for the DAX Replica Store. */
    private static final String DAX_REPLICA_STORE_SUFFIX = ".replica.store";

    /** The bag of initialization objects. */
    protected PegasusBag mBag;

    /** The directory where all the submit files are to be generated. */
    protected String mSubmitFileDir;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The object containing the command line options specified to the planner at runtime. */
    protected PlannerOptions mPOptions;

    /** The handle to the logging object. */
    protected LogManager mLogger;

    /**
     * Returns the path to the DAX Replica Store File.
     *
     * @param options the options for the sub workflow.
     * @param label the label for the workflow.
     * @param index the index for the workflow.
     * @return the name of the cache file
     */
    public static String getDAXReplicaStoreFile(
            PlannerOptions options, String label, String index) {
        StringBuffer sb = new StringBuffer();
        sb.append(options.getSubmitDirectory())
                .append(File.separator)
                .append(
                        Abstract.getDAGFilename(
                                options, label, index, DAXReplicaStore.DAX_REPLICA_STORE_SUFFIX));

        return sb.toString();
    }

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag the bag of initialization objects.
     * @throws CodeGeneratorException in case of any error occurring code generation.
     */
    public void initialize(PegasusBag bag) throws CodeGeneratorException {
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger = bag.getLogger();
    }

    /**
     * Generates the notifications input file. The method initially generates work-flow level
     * notification records, followed by job-level notification records.
     *
     * @param dag the concrete work-flow.
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occurring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException {

        // sanity check
        if (dag.getReplicaStore().isEmpty()) {
            return new LinkedList<File>();
        }

        ReplicaCatalog rc = null;
        Properties replicaStoreProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);
        File file =
                new File(getDAXReplicaStoreFile(this.mPOptions, dag.getLabel(), dag.getIndex()));

        // set the appropriate property to designate path to file
        replicaStoreProps.setProperty(
                DAXReplicaStore.DAX_REPLICA_STORE_CATALOG_KEY, file.getAbsolutePath());

        mLogger.log(
                "Writing out the DAX Replica Store to file " + file.getAbsolutePath(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        try {
            rc =
                    ReplicaFactory.loadInstance(
                            DAXReplicaStore.DAX_REPLICA_STORE_CATALOG_IMPLEMENTER,
                            replicaStoreProps);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize the DAX Replica Store File  " + file, e);
        }

        // get hold of DAX Replica Store
        ReplicaStore store = dag.getReplicaStore();
        for (Iterator it = store.replicaLocationIterator(); it.hasNext(); ) {
            ReplicaLocation rl = (ReplicaLocation) it.next();
            String lfn = rl.getLFN();
            for (Iterator rceIt = rl.pfnIterator(); rceIt.hasNext(); ) {
                ReplicaCatalogEntry rce = (ReplicaCatalogEntry) rceIt.next();
                rc.insert(lfn, rce);
            }
        }
        rc.close();

        Collection<File> result = new LinkedList<File>();
        result.add(file);
        return result;
    }

    /**
     * Not implemented
     *
     * @param dag the work-flow
     * @param job the job for which the code is to be generated.
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    public void generateCode(ADag dag, Job job) throws CodeGeneratorException {
        throw new CodeGeneratorException(
                "Replica Store generator only generates code for the whole workflow");
    }

    /** Not implemented */
    public boolean startMonitoring() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** Not implemented */
    public void reset() throws CodeGeneratorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
