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
package edu.isi.pegasus.planner.partitioner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PartitionerFactoryTest {

    @Test
    public void testConstants() {
        assertThat(
                PartitionerFactory.DEFAULT_PACKAGE_NAME, is("edu.isi.pegasus.planner.partitioner"));
        assertThat(PartitionerFactory.LEVEL_BASED_PARTITIONING_CLASS, is("BFS"));
        assertThat(PartitionerFactory.LABEL_BASED_PARTITIONING_CLASS, is("Label"));
        assertThat(PartitionerFactory.HORIZONTAL_PARTITIONING_CLASS, is("Horizontal"));
        assertThat(PartitionerFactory.WHOLE_WF_PARTITIONING_CLASS, is("Whole"));
        assertThat(PartitionerFactory.DEFAULT_PARTITIONING_CLASS, is("BFS"));
    }

    @Test
    public void testLoadInstanceWithNullPropertiesThrows() {
        assertThrows(
                NullPointerException.class,
                () ->
                        PartitionerFactory.loadInstance(
                                null, new GraphNode("root"), new HashMap<>(), "Label"));
    }

    @Test
    public void testLoadInstanceWithCaseInsensitiveShortClassNameLoadsLabel() throws Exception {
        Partitioner partitioner =
                PartitionerFactory.loadInstance(
                        PegasusProperties.nonSingletonInstance(),
                        new GraphNode("root"),
                        new HashMap<>(),
                        "Label");

        assertThat(partitioner, is(notNullValue()));
        assertThat(partitioner.getClass(), is(Label.class));
    }

    @Test
    public void testLoadInstanceWithFullyQualifiedClassNameLoadsLabel() throws Exception {
        Partitioner partitioner =
                PartitionerFactory.loadInstance(
                        PegasusProperties.nonSingletonInstance(),
                        new GraphNode("root"),
                        new HashMap<>(),
                        "edu.isi.pegasus.planner.partitioner.Label");

        assertThat(partitioner, is(notNullValue()));
        assertThat(partitioner.getClass(), is(Label.class));
    }
}
