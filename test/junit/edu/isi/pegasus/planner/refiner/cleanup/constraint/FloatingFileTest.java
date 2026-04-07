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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for {@link FloatingFile}. */
public class FloatingFileTest {

    @Test
    public void testConstructorSetsDependencies() {
        Set<GraphNode> deps = new HashSet<>();
        deps.add(new GraphNode("node-1"));
        PegasusFile pf = new PegasusFile("output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertThat(ff.dependencies, sameInstance(deps));
    }

    @Test
    public void testConstructorSetsFile() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("data.bin");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertThat(ff.file, sameInstance(pf));
    }

    @Test
    public void testFileReturnsCorrectLFN() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("result.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertThat(ff.file.getLFN(), is("result.txt"));
    }

    @Test
    public void testEmptyDependenciesAllowed() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("root-output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertThat(ff.dependencies.isEmpty(), is(true));
    }

    @Test
    public void testMultipleDependencies() {
        Set<GraphNode> deps = new HashSet<>();
        deps.add(new GraphNode("parent-1"));
        deps.add(new GraphNode("parent-2"));
        deps.add(new GraphNode("parent-3"));
        PegasusFile pf = new PegasusFile("output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertThat(ff.dependencies.size(), is(3));
    }

    @Test
    public void testConstructorAllowsNullValues() {
        FloatingFile ff = new FloatingFile(null, null);
        assertThat(ff.dependencies, nullValue());
        assertThat(ff.file, nullValue());
    }

    @Test
    public void testFieldsArePublicAndFinal() throws Exception {
        java.lang.reflect.Field dependencies = FloatingFile.class.getDeclaredField("dependencies");
        java.lang.reflect.Field file = FloatingFile.class.getDeclaredField("file");

        assertThat(java.lang.reflect.Modifier.isPublic(dependencies.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isFinal(dependencies.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isPublic(file.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isFinal(file.getModifiers()), is(true));
    }

    @Test
    public void testDeclaresOnlyExpectedFields() {
        assertThat(FloatingFile.class.getDeclaredFields().length, is(2));
    }
}
