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
package edu.isi.pegasus.planner.parser;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Parses the input stream and generates the TransformationStore as output.
 *
 * <p>This parser is able to parse the Transformation Catalog specification in the following format
 *
 * <pre>
 * pegasus: "5.0"
 * transformations:
 *    - namespace: "example"
 *      name: "keg"
 *      version: "1.0"
 *      profiles:
 *          env:
 *              APP_HOME: "/tmp/myscratch"
 *              JAVA_HOME: "/opt/java/1.6"
 *          pegasus:
 *              clusters.num: "1"
 *
 *      requires:
 *          - anotherTr
 *
 *      sites:
 *       - name: "isi"
 *          type: "installed"
 *          pfn: "/path/to/keg"
 *          arch: "x86_64"
 *          os.type: "linux"
 *          os.release: "fc"
 *          os.version: "1.0"
 *          profiles:
 *            env:
 *                Hello: World
 *                JAVA_HOME: /bin/java.1.6
 *            condor:
 *                FOO: bar
 *          container: centos-pegasus
 *
 *    - namespace: example
 *      name: anotherTr
 *      version: "1.2.3"
 *
 *      sites:
 *          - name: isi
 *            type: installed
 *            pfn: /path/to/anotherTr
 *
 * containers:
 *    - name: centos-pegasus
 *      type: docker
 *      image: docker:///rynge/montage:latest
 *      mount: /Volumes/Work/lfs1:/shared-data/:ro
 *      mount: /Volumes/Work/lfs12:/shared-data1/:ro
 *      profiles:
 *          env:
 *              JAVA_HOME: /opt/java/1.6
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TransformationCatalogYAMLParser extends YAMLParser {

    /** Schema file name; */
    private static final String SCHEMA_URI = "http://pegasus.isi.edu/schema/tc-5.0.yml";

    /** Schema File Object; */
    private static File SCHEMA_FILENAME = null;

    /**
     * Initializes the parser with an input stream to read from.
     *
     * @param bag
     * @param schemaDir
     * @throws IOException
     * @throws ScannerException
     */
    public TransformationCatalogYAMLParser(PegasusBag bag, File schemaDir)
            throws IOException, ScannerException {
        super(bag);
        File yamlSchemaDir = new File(schemaDir, "yaml");
        SCHEMA_FILENAME = new File(yamlSchemaDir, new File(SCHEMA_URI).getName());
    }

    /**
     * Parses the complete input stream
     *
     * @param file
     * @param modifyFileURL Boolean indicating whether to modify the file URL or not
     * @return TransformationStore
     * @throws ScannerException
     * @throws
     */
    @SuppressWarnings("unchecked")
    public TransformationStore parse(String file, boolean modifyFileURL) throws ScannerException {
        TransformationStore store = new TransformationStore();
        File f = new File(file);
        if (!(f.exists() && f.length() > 0)) {
            mLogger.log(
                    "The Transformation Catalog file " + file + " was not found or empty",
                    LogManager.INFO_MESSAGE_LEVEL);
            return store;
        }
        try {
            // first attempt to validate
            if (validate(f, SCHEMA_FILENAME, "transformation")) {
                // validation succeeded. load.
                Reader reader = new VariableExpansionReader(new FileReader(f));
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                store = mapper.readValue(reader, TransformationStore.class);
                for (TransformationCatalogEntry entry : store.getAllEntries()) {
                    if (modifyFileURL) {
                        Abstract.modifyForFileURLS(entry);
                    }
                    // we have information about one transformation catalog c
                    mLogger.log(
                            "Transformation Catalog Entry parsed is - " + entry,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                }
                for (Container c : store.getAllContainers()) {
                    // we have information about one transformation catalog c
                    mLogger.log("Container Entry parsed is - " + c, LogManager.DEBUG_MESSAGE_LEVEL);
                }
            }
        } catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        }
        return store;
    }
}
