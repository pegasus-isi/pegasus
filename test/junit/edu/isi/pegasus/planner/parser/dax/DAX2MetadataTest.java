/**
 * Copyright 2007-2013 University Of Southern California
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DAX2MetadataTest {

    @Test
    public void getConstructedObjectBeforeParsingThrows() {
        DAX2Metadata metadata = new DAX2Metadata();

        RuntimeException exception =
                assertThrows(RuntimeException.class, metadata::getConstructedObject);

        assertThat(exception.getMessage(), is("Method called before the metadata was parsed"));
    }

    @Test
    public void cbDocumentStoresParsedMetadataAndDefaults() {
        DAX2Metadata metadata = new DAX2Metadata();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("name", "diamond");
        attributes.put("version", "5.0.4");

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> metadata.cbDocument(attributes));

        assertThat(exception.getMessage(), is(DAX2Metadata.PARSING_DONE_ERROR_MESSAGE));

        Map<?, ?> constructed = (Map<?, ?>) metadata.getConstructedObject();
        assertThat(constructed.get("name"), is("diamond"));
        assertThat(constructed.get("version"), is("5.0.4"));
        assertThat(constructed.get("index"), is(DAX2Metadata.DEFAULT_ADAG_INDEX_ATTRIBUTE));
        assertThat(constructed.get("count"), is(DAX2Metadata.DEFAULT_ADAG_COUNT_ATTRIBUTE));
    }

    @Test
    public void cbDocumentPreservesExplicitCountAndIndex() {
        DAX2Metadata metadata = new DAX2Metadata();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("name", "partitioned");
        attributes.put("version", "5.1.0");
        attributes.put("index", "2");
        attributes.put("count", "7");

        assertThrows(RuntimeException.class, () -> metadata.cbDocument(attributes));

        Map<?, ?> constructed = (Map<?, ?>) metadata.getConstructedObject();
        assertThat(constructed.get("index"), is("2"));
        assertThat(constructed.get("count"), is("7"));
    }

    @Test
    public void initializeAcceptsPegasusBag() {
        DAX2Metadata metadata = new DAX2Metadata();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());

        assertDoesNotThrow(() -> metadata.initialize(bag, "workflow.dax"));
    }

    @Test
    public void cbMetadataIsANoOp() {
        DAX2Metadata metadata = new DAX2Metadata();

        assertDoesNotThrow(() -> metadata.cbMetadata(null));
    }

    @Test
    public void unsupportedCallbacksThrowUnsupportedOperationException() {
        DAX2Metadata metadata = new DAX2Metadata();

        assertThrows(UnsupportedOperationException.class, () -> metadata.cbWfInvoke(null));
        assertThrows(
                UnsupportedOperationException.class, () -> metadata.cbCompoundTransformation(null));
        assertThrows(UnsupportedOperationException.class, () -> metadata.cbFile(null));
        assertThrows(UnsupportedOperationException.class, () -> metadata.cbExecutable(null));
        assertThrows(UnsupportedOperationException.class, () -> metadata.cbChildren("a", null));
        assertThrows(UnsupportedOperationException.class, () -> metadata.cbReplicaStore(null));
        assertThrows(
                UnsupportedOperationException.class, () -> metadata.cbTransformationStore(null));
        assertThrows(UnsupportedOperationException.class, () -> metadata.cbSiteStore(null));
    }
}
