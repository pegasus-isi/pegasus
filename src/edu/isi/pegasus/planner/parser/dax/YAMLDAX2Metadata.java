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

import static edu.isi.pegasus.planner.parser.dax.DAX2Metadata.DEFAULT_ADAG_COUNT_ATTRIBUTE;
import static edu.isi.pegasus.planner.parser.dax.DAX2Metadata.DEFAULT_ADAG_INDEX_ATTRIBUTE;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.WorkflowKeywords;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A convenience class to just parse the top level document attributes from a DAX file. Avoid
 * loading large workflows into memory, as it stream the document in.
 *
 * <pre>
 * x-pegasus: {apiLang: python, createdBy: bamboo, createdOn: '07-08-21T10:08:23Z'}
 * pegasus: '5.0.4'
 * name: local-hierarchy
 * jobs:
 *   - type: pegasusWorkflow
 * ...
 * </pre>
 *
 * Stops parsing once you reach catalogs or jobs section.
 *
 * @author Karan Vahi
 */
public class YAMLDAX2Metadata {

    public YAMLDAX2Metadata() {}

    public static final Map<String, String> getMetadata(PegasusBag bag, String dax) {
        Map<String, String> attributes = new HashMap();

        attributes.put("index", DEFAULT_ADAG_INDEX_ATTRIBUTE);
        attributes.put("count", DEFAULT_ADAG_COUNT_ATTRIBUTE);

        YAMLFactory factory = new YAMLFactory();

        JsonParser parser = null;
        try {
            parser = factory.createParser(new File(dax));

            // sample stuff to parse
            // x-pegasus: {apiLang: python, createdBy: bamboo, createdOn: '07-08-21T10:08:23Z'}
            // pegasus: '5.0.4'
            // name: local-hierarchy
            // jobs:
            // - type: pegasusWorkflow

            // return the following
            // {count=1, name=blackdiamond, index=0, version=5.0}

            // Iterate through the tokens/events in the stream
            boolean shortCircuit = false;
            while (parser.nextToken() != null) {
                if (shortCircuit || attributes.size() == 4) {
                    // no need to parse further
                    break;
                }
                // Process data based on the token type (e.g., FIELD_NAME, VALUE_STRING)
                String fieldName = parser.getCurrentName();
                System.out.println("Found token " + fieldName);

                WorkflowKeywords reservedKey = WorkflowKeywords.getReservedKey(fieldName);
                if (reservedKey == null) {
                    continue;
                }

                switch (reservedKey) {
                    case PEGASUS:
                        parser.nextToken();
                        attributes.put("version", parser.getText());
                        break;

                    case NAME:
                        parser.nextToken();
                        attributes.put("name", parser.getText());
                        break;

                    case JOBS:
                        shortCircuit = true;
                        break;

                    case REPLICA_CATALOG:
                        shortCircuit = true;
                        break;

                    case SITE_CATALOG:
                        shortCircuit = true;
                        break;

                    case TRANSFORMATION_CATALOG:
                        shortCircuit = true;
                        break;

                    default:
                        break;
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Exception while reading file for document level metadata " + dax, ioe);
        } finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException ex) {
                }
            }
        }
        return attributes;
    }

    public static void main(String[] args) {
        YAMLDAX2Metadata ym = new YAMLDAX2Metadata();
        String dax =
                "/Volumes/lfs1/work/pegasus-features/bugs/045-hierarchy-sharedfs-c/workflow.yml";
        YAMLDAX2Metadata.getMetadata(null, dax);
    }
}
