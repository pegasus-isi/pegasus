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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class BFSTest {

    private static final class RecordingCallback implements Callback {
        Partition partition;
        String childId;
        List parents;
        boolean done;

        @Override
        public void cbPartition(Partition p) {
            this.partition = p;
        }

        @Override
        public void cbParents(String child, List parents) {
            this.childId = child;
            this.parents = parents;
        }

        @Override
        public void cbDone() {
            this.done = true;
        }
    }

    @Test
    public void testDescriptionReturnsConstantDescription() {
        BFS bfs =
                new BFS(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        assertThat(bfs.description(), is(BFS.DESCRIPTION));
    }

    @Test
    public void testConstructorInitializesQueueAndCurrentDepth() throws Exception {
        BFS bfs =
                new BFS(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        Object queue = ReflectionTestUtils.getField(bfs, "mQueue");
        Object depth = ReflectionTestUtils.getField(bfs, "mCurrentDepth");

        assertThat(queue, is(notNullValue()));
        assertThat(queue instanceof LinkedList, is(true));
        assertThat(depth, is(-1));
    }

    @Test
    public void testConstructLevelRelationsCallsCallbackWithExpectedIds() {
        BFS bfs =
                new BFS(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        RecordingCallback callback = new RecordingCallback();

        bfs.constructLevelRelations(callback, 2, 3);

        assertThat(callback.childId, is("ID3"));
        assertThat(callback.parents, is(notNullValue()));
        assertThat(callback.parents.size(), is(1));
        assertThat(callback.parents.get(0), is("ID2"));
    }

    @Test
    public void testDoneCallsCallbackDone() {
        BFS bfs =
                new BFS(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        RecordingCallback callback = new RecordingCallback();

        bfs.done(callback);

        assertThat(callback.done, is(true));
    }

    @Test
    public void testPrivateGetPartitionIDFormatsIdentifier() throws Exception {
        BFS bfs =
                new BFS(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        Method method = BFS.class.getDeclaredMethod("getPartitionID", int.class);
        method.setAccessible(true);

        assertThat(method.invoke(bfs, 4), is("ID4"));
    }
}
