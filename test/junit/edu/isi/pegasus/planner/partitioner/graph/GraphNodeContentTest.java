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
package edu.isi.pegasus.planner.partitioner.graph;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the GraphNodeContent interface.
 *
 * @author Rajiv Mayani
 */
public class GraphNodeContentTest {

    @Test
    public void testGraphNodeContentIsInterface() {
        assertTrue(GraphNodeContent.class.isInterface(), "GraphNodeContent should be an interface");
    }

    @Test
    public void testHasSetGraphNodeReferenceMethod() throws NoSuchMethodException {
        Method m = GraphNodeContent.class.getMethod("setGraphNodeReference", GraphNode.class);
        assertNotNull(m, "GraphNodeContent should declare setGraphNodeReference(GraphNode)");
    }

    @Test
    public void testSetGraphNodeReferenceMethodIsPublic() throws NoSuchMethodException {
        Method m = GraphNodeContent.class.getMethod("setGraphNodeReference", GraphNode.class);
        assertTrue(Modifier.isPublic(m.getModifiers()), "setGraphNodeReference should be public");
    }

    @Test
    public void testGraphNodeHasGetContentMethod() throws NoSuchMethodException {
        Method m = GraphNode.class.getMethod("getContent");
        assertEquals(
                GraphNodeContent.class,
                m.getReturnType(),
                "getContent() should return a GraphNodeContent");
    }

    @Test
    public void testGraphNodeHasSetContentMethod() throws NoSuchMethodException {
        Method m = GraphNode.class.getMethod("setContent", GraphNodeContent.class);
        assertNotNull(m, "GraphNode should have a setContent(GraphNodeContent) method");
    }

    @Test
    public void testGraphNodeContentPackage() {
        assertEquals(
                "edu.isi.pegasus.planner.partitioner.graph",
                GraphNodeContent.class.getPackage().getName(),
                "GraphNodeContent should be in the correct package");
    }

    @Test
    public void testDefaultContentIsNullForNewNode() {
        GraphNode node = new GraphNode("X", "job");
        assertNull(
                node.getContent(), "Default content should be null for a newly created GraphNode");
    }

    @Test
    public void testSetContentStoresValue() {
        GraphNode node = new GraphNode("X", "job");
        // Use a lambda/anonymous class since it's just an interface
        GraphNodeContent content = n -> {};
        node.setContent(content);
        assertNotNull(node.getContent(), "getContent should return the set content");
    }
}
