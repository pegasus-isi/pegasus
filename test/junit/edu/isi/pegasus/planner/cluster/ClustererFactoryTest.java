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
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.Partitioner;
import edu.isi.pegasus.planner.partitioner.Whole;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for ClustererFactory constants and structural validation. */
public class ClustererFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertThat(ClustererFactory.DEFAULT_PACKAGE_NAME, is("edu.isi.pegasus.planner.cluster"));
    }

    @Test
    public void testHorizontalClusteringClass() {
        assertThat(ClustererFactory.HORIZONTAL_CLUSTERING_CLASS, is("Horizontal"));
    }

    @Test
    public void testVerticalClusteringClass() {
        assertThat(ClustererFactory.VERTICAL_CLUSTERING_CLASS, is("Vertical"));
    }

    @Test
    public void testHorizontalClusteringClassNotNull() {
        assertThat(ClustererFactory.HORIZONTAL_CLUSTERING_CLASS, notNullValue());
    }

    @Test
    public void testVerticalClusteringClassNotNull() {
        assertThat(ClustererFactory.VERTICAL_CLUSTERING_CLASS, notNullValue());
    }

    @Test
    public void testLoadClustererWithNullTypeThrows() {
        assertThrows(
                ClustererFactoryException.class,
                () -> ClustererFactory.loadClusterer(null, null, null),
                "loadClusterer with null type should throw ClustererFactoryException");
    }

    @Test
    public void testLoadClustererWithUnknownTypeThrows() {
        assertThrows(
                ClustererFactoryException.class,
                () -> ClustererFactory.loadClusterer(null, null, "nonexistent-clustering"),
                "loadClusterer with unknown type should throw ClustererFactoryException");
    }

    @Test
    public void testLoadPartitionerWithNullTypeThrows() {
        assertThrows(
                ClustererFactoryException.class,
                () -> ClustererFactory.loadPartitioner(null, null, null, null),
                "loadPartitioner with null type should throw ClustererFactoryException");
    }

    @Test
    public void testLoadPartitionerWithUnknownTypeThrows() {
        ClustererFactoryException exception =
                assertThrows(
                        ClustererFactoryException.class,
                        () ->
                                ClustererFactory.loadPartitioner(
                                        properties(),
                                        "unknown-technique",
                                        new GraphNode(),
                                        graph()),
                        "unknown clustering techniques should be rejected");

        assertThat(exception.getMessage().contains("No matching partitioner found"), is(true));
    }

    @Test
    public void testLoadPartitionerLoadsBFSForHorizontalType() throws Exception {
        Partitioner partitioner =
                ClustererFactory.loadPartitioner(
                        properties(), "horizontal", new GraphNode("root"), graph());

        assertThat(partitioner.getClass().getName(), is("edu.isi.pegasus.planner.partitioner.BFS"));
    }

    @Test
    public void testLoadPartitionerLoadsWholeForWholeType() throws Exception {
        Partitioner partitioner =
                ClustererFactory.loadPartitioner(
                        properties(), "whole", new GraphNode("root"), graph());

        assertThat(partitioner, instanceOf(Whole.class));
    }

    @Test
    public void testClustererTableMapsHorizontalTypeToHorizontalClass() throws Exception {
        Map table = clustererTable();

        assertThat(table.get("horizontal"), is(ClustererFactory.HORIZONTAL_CLUSTERING_CLASS));
    }

    @Test
    public void testClustererTableMapsLabelAndWholeTypesToVerticalClass() throws Exception {
        Map table = clustererTable();

        assertThat(table.get("label"), is(ClustererFactory.VERTICAL_CLUSTERING_CLASS));
        assertThat(table.get("whole"), is(ClustererFactory.VERTICAL_CLUSTERING_CLASS));
    }

    private PegasusBag bag() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties properties = properties();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(new File("build"));
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        try {
            bag.add(
                    PegasusBag.PEGASUS_LOGMANAGER,
                    LogManagerFactory.loadSingletonInstance(properties));
        } catch (Exception e) {
            throw new RuntimeException("Unable to create test logger", e);
        }
        return bag;
    }

    private PegasusProperties properties() {
        return PegasusProperties.nonSingletonInstance();
    }

    private Map graph() {
        return new HashMap();
    }

    private Map clustererTable() throws Exception {
        java.lang.reflect.Method method =
                ClustererFactory.class.getDeclaredMethod("clustererTable");
        method.setAccessible(true);
        return (Map) method.invoke(null);
    }
}
