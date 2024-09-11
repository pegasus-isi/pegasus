/*
 *
 *   Copyright 2007-2020 University Of Southern California
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
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** @author Karan Vahi */
public class TransformationStoreTest {

    private TestSetup mTestSetup;

    public TransformationStoreTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterEach
    public void tearDown() {}

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}

    @Test
    public void testDifferentTXNameWithDockerContainer() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        Container c = new Container("centos-pegasus");
        c.setImageURL("docker:///pegasus/centos-pegasus:latest");
        c.setType(Container.TYPE.docker);
        entry.setContainer(c);
        TransformationStore store = new TransformationStore();
        store.sanityCheck(entry);
    }

    @ParameterizedTest
    @CsvSource({
        "pegasus-keg, docker, centos-pegasus, docker:///pegasus/centos-pegasus:latest",
        "pegasus-keg, singularity, centos-pegasus, docker:///pegasus/centos-pegasus:latest",
        "pegasus-keg, singularity, centos-pegasus, shub:///pegasus/centos-pegasus:latest",
        // following only works in singularity because a suffix is added as suffix to singularity
        // lfn
        "centos-pegasus, singularity, centos-pegasus, docker:///pegasus/centos-pegasus:latest",
        "centos-pegasus, singularity, centos-pegasus, http:///pegasus.isi.edu/images/singularity/centos-pegasus.tar",
    })
    public void testDifferentTXNameWithContainer(
            String txName, String containerType, String containerName, String imageURL) {
        TransformationCatalogEntry entry = new TransformationCatalogEntry(null, txName, null);
        Container c = new Container(containerName);
        c.setImageURL(imageURL);
        c.setType(Container.TYPE.valueOf(containerType));
        entry.setContainer(c);
        TransformationStore store = new TransformationStore();
        store.sanityCheck(entry);
        assertTrue(containerName.contentEquals(c.getLFN()));
    }

    @ParameterizedTest
    @CsvSource({
        "centos-pegasus, docker, centos-pegasus, docker:///pegasus/centos-pegasus:latest",
        "centos-pegasus.tar, singularity, centos-pegasus, http:///pegasus.isi.edu/images/singularity/centos-pegasus.tar",
    })
    public void testSameTXNameWithContainer(
            String txName, String containerType, String containerName, String imageURL) {
        TransformationCatalogEntry entry = new TransformationCatalogEntry(null, txName, null);
        Container c = new Container(containerName);
        c.setImageURL(imageURL);
        c.setType(Container.TYPE.valueOf(containerType));
        entry.setContainer(c);
        TransformationStore store = new TransformationStore();

        Exception e =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> {
                            store.sanityCheck(entry);
                        });
        assertTrue(
                e.getMessage().contains("matches associated container's computed LFN"),
                "Exception thrown was " + e);
    }
}
