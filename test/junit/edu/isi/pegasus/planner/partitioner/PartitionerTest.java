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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class PartitionerTest {

    @Test
    public void testPartitionerIsAbstract() {
        assertThat(Modifier.isAbstract(Partitioner.class.getModifiers()), is(true));
    }

    private static final class StubPartitioner extends Partitioner {

        StubPartitioner(GraphNode root, Map graph, PegasusProperties properties) {
            super(root, graph, properties);
        }

        @Override
        public void determinePartitions(Callback c) {}

        @Override
        public String description() {
            return "stub";
        }
    }

    @Test
    public void testConstants() {
        assertThat(Partitioner.PACKAGE_NAME, is("edu.isi.pegasus.planner.partitioner"));
        assertThat(Partitioner.VERSION, is("1.2"));
    }

    @Test
    public void testConstructorStoresRootGraphLoggerAndProperties() throws Exception {
        GraphNode root = new GraphNode("root");
        Map<String, GraphNode> graph = new HashMap<>();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        StubPartitioner partitioner = new StubPartitioner(root, graph, properties);

        assertThat(ReflectionTestUtils.getField(partitioner, "mRoot"), is(sameInstance(root)));
        assertThat(ReflectionTestUtils.getField(partitioner, "mGraph"), is(sameInstance(graph)));
        assertThat(
                ReflectionTestUtils.getField(partitioner, "mProps"), is(sameInstance(properties)));
        assertThat(ReflectionTestUtils.getField(partitioner, "mLogger"), is(notNullValue()));
        assertThat(
                ReflectionTestUtils.getField(partitioner, "mLogger") instanceof LogManager,
                is(true));
    }

    @Test
    public void testDeclaredAbstractMethodsAndConstructorSignature() throws Exception {
        Method determinePartitions =
                Partitioner.class.getDeclaredMethod("determinePartitions", Callback.class);
        Method description = Partitioner.class.getDeclaredMethod("description");

        assertThat(Modifier.isAbstract(determinePartitions.getModifiers()), is(true));
        assertThat(determinePartitions.getReturnType(), is(void.class));
        assertThat(Modifier.isAbstract(description.getModifiers()), is(true));
        assertThat(description.getReturnType(), is(String.class));

        assertThat(
                Partitioner.class.getDeclaredConstructor(
                        GraphNode.class, Map.class, PegasusProperties.class),
                is(notNullValue()));
    }
}
