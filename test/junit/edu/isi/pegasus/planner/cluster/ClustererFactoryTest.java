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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for ClustererFactory constants and structural validation. */
public class ClustererFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.cluster",
                ClustererFactory.DEFAULT_PACKAGE_NAME,
                "Default package name should match");
    }

    @Test
    public void testHorizontalClusteringClass() {
        assertEquals(
                "Horizontal",
                ClustererFactory.HORIZONTAL_CLUSTERING_CLASS,
                "HORIZONTAL_CLUSTERING_CLASS should be 'Horizontal'");
    }

    @Test
    public void testVerticalClusteringClass() {
        assertEquals(
                "Vertical",
                ClustererFactory.VERTICAL_CLUSTERING_CLASS,
                "VERTICAL_CLUSTERING_CLASS should be 'Vertical'");
    }

    @Test
    public void testHorizontalClusteringClassNotNull() {
        assertNotNull(
                ClustererFactory.HORIZONTAL_CLUSTERING_CLASS,
                "HORIZONTAL_CLUSTERING_CLASS constant should not be null");
    }

    @Test
    public void testVerticalClusteringClassNotNull() {
        assertNotNull(
                ClustererFactory.VERTICAL_CLUSTERING_CLASS,
                "VERTICAL_CLUSTERING_CLASS constant should not be null");
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
}
