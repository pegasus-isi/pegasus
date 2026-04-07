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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
    public void testHasSetGraphNodeReferenceMethod() throws NoSuchMethodException {
        Method m = GraphNodeContent.class.getMethod("setGraphNodeReference", GraphNode.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testSetGraphNodeReferenceMethodIsPublic() throws NoSuchMethodException {
        Method m = GraphNodeContent.class.getMethod("setGraphNodeReference", GraphNode.class);
        assertThat(Modifier.isPublic(m.getModifiers()), is(true));
    }

    @Test
    public void testGraphNodeHasGetContentMethod() throws NoSuchMethodException {
        Method m = GraphNode.class.getMethod("getContent");
        assertThat(m.getReturnType(), is(GraphNodeContent.class));
    }

    @Test
    public void testGraphNodeHasSetContentMethod() throws NoSuchMethodException {
        Method m = GraphNode.class.getMethod("setContent", GraphNodeContent.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphNodeContentPackage() {
        assertThat(
                GraphNodeContent.class.getPackage().getName(),
                is("edu.isi.pegasus.planner.partitioner.graph"));
    }

    @Test
    public void testDefaultContentIsNullForNewNode() {
        GraphNode node = new GraphNode("X", "job");
        assertThat(node.getContent(), is(nullValue()));
    }

    @Test
    public void testSetContentStoresValue() {
        GraphNode node = new GraphNode("X", "job");
        // Use a lambda/anonymous class since it's just an interface
        GraphNodeContent content = n -> {};
        node.setContent(content);
        assertThat(node.getContent(), is(notNullValue()));
    }

    @Test
    public void testGraphNodeContentIsInterface() {
        assertThat(GraphNodeContent.class.isInterface(), is(true));
    }

    @Test
    public void testSetGraphNodeReferenceReturnsVoid() throws NoSuchMethodException {
        Method m = GraphNodeContent.class.getMethod("setGraphNodeReference", GraphNode.class);
        assertThat(m.getReturnType(), is(void.class));
    }

    @Test
    public void testGraphNodeContentDeclaresOnlySingleMethod() {
        assertThat(GraphNodeContent.class.getDeclaredMethods().length, is(1));
    }
}
