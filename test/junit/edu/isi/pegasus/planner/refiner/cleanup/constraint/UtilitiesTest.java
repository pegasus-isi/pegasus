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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for {@link Utilities}. */
public class UtilitiesTest {

    @Test
    public void testGetFileSizeWhenSizeSet() {
        PegasusFile pf = new PegasusFile("myfile.txt");
        pf.setSize(2048.0);
        long size = Utilities.getFileSize(pf);
        assertThat(size, is(2048L));
    }

    @Test
    public void testGetFileSizeDefaultWhenNoSize() {
        PegasusFile pf = new PegasusFile("unknown.txt");
        // size is -1 by default so falls back to DEFAULT_FILE_SIZE (10 MB)
        long size = Utilities.getFileSize(pf);
        assertThat(size, is(10485760L));
    }

    @Test
    public void testGetFileSizeWhenSizeIsZero() {
        PegasusFile pf = new PegasusFile("empty.txt");
        pf.setSize(0.0);
        long size = Utilities.getFileSize(pf);
        assertThat(size, is(0L));
    }

    @Test
    public void testGetFileSizeLargeFile() {
        PegasusFile pf = new PegasusFile("big.bin");
        pf.setSize(1073741824.0); // 1 GB
        long size = Utilities.getFileSize(pf);
        assertThat(size, is(1073741824L));
    }

    @Test
    public void testSizesMapInitiallyNull() {
        // Accessing getFileSize with sizes=null (or undefined files) should use fallback
        PegasusFile pf = new PegasusFile("not-in-csv.dat");
        long size = Utilities.getFileSize(pf);
        // Should be either the file's own size or the DEFAULT_FILE_SIZE
        assertThat(size > 0, is(true));
    }

    @Test
    public void testLoadHashMapReadsCsvSizes() throws Exception {
        ReflectionTestUtils.setField(Utilities.class, "sizes", null);

        Path csv = Files.createTempFile("utilities-sizes", ".csv");
        Files.write(
                csv,
                Collections.singletonList(
                        "transformation,id,type,filename,length\nx,1,input,data.txt,321"),
                StandardCharsets.UTF_8);

        Utilities.loadHashMap(csv.toString());

        PegasusFile pf = new PegasusFile("data.txt");
        assertThat(Utilities.getFileSize(pf), is(321L));

        ReflectionTestUtils.setField(Utilities.class, "sizes", null);
    }

    @Test
    public void testCleanUpJobToStringIncludesParentsChildrenAndSizes() throws Exception {
        ReflectionTestUtils.setField(Utilities.class, "sizes", null);

        GraphNode parent = new GraphNode("parent");
        GraphNode child = new GraphNode("child");
        PegasusFile file = new PegasusFile("f.txt");
        file.setSize(12.0);

        String value =
                Utilities.cleanUpJobToString(
                        Collections.singletonList(parent),
                        Collections.singletonList(child),
                        Collections.singletonList(file));

        assertThat(
                value,
                is("CleanupJob{parents = {parent}, children = {child}, files = {f.txt:12}}"));
    }

    @Test
    public void testGetIntermediateRequirementHandlesJobTypes() throws Exception {
        ReflectionTestUtils.setField(Utilities.class, "sizes", null);

        PegasusFile input = new PegasusFile("input.dat");
        input.setSize(10.0);
        PegasusFile output = new PegasusFile("output.dat");
        output.setSize(25.0);

        Job cleanup = new Job();
        cleanup.setJobType(Job.CLEANUP_JOB);
        cleanup.setInputFiles(new HashSet<PegasusFile>(Collections.singletonList(input)));

        Job stageOut = new Job();
        stageOut.setJobType(Job.STAGE_OUT_JOB);

        Job compute = new Job();
        compute.setJobType(Job.COMPUTE_JOB);
        compute.setOutputFiles(new HashSet<PegasusFile>(Collections.singletonList(output)));

        assertThat(Utilities.getIntermediateRequirement(cleanup), is(-10L));
        assertThat(Utilities.getIntermediateRequirement(stageOut), is(0L));
        assertThat(Utilities.getIntermediateRequirement(compute), is(25L));
    }
}
