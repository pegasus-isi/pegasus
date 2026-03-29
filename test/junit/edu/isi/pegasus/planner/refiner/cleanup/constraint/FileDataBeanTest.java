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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FileDataBean} data bean class. */
public class FileDataBeanTest {

    private FileDataBean bean;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        bean = new FileDataBean();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultFieldsAreNull() {
        assertNull(bean.transformation);
        assertNull(bean.id);
        assertNull(bean.type);
        assertNull(bean.filename);
        assertEquals(0L, bean.length);
    }

    @Test
    public void testSetTransformation() {
        bean.setTransformation("blackdiamond::preprocess");
        assertEquals("blackdiamond::preprocess", bean.transformation);
    }

    @Test
    public void testSetId() {
        bean.setId("job-001");
        assertEquals("job-001", bean.id);
    }

    @Test
    public void testSetType() {
        bean.setType("input");
        assertEquals("input", bean.type);
    }

    @Test
    public void testSetFilename() {
        bean.setFilename("input.txt");
        assertEquals("input.txt", bean.filename);
    }

    @Test
    public void testSetLength() {
        bean.setLength(1048576L);
        assertEquals(1048576L, bean.length);
    }

    @Test
    public void testToStringContainsAllFields() {
        bean.setTransformation("myjob");
        bean.setId("id-42");
        bean.setType("output");
        bean.setFilename("output.dat");
        bean.setLength(512L);
        String s = bean.toString();
        assertThat(s, containsString("myjob"));
        assertThat(s, containsString("id-42"));
        assertThat(s, containsString("output.dat"));
        assertThat(s, containsString("512"));
    }

    @Test
    public void testDirectFieldAssignment() {
        bean.transformation = "mypfn";
        bean.id = "job-99";
        bean.type = "io";
        bean.filename = "file.bin";
        bean.length = 2048L;
        assertEquals("mypfn", bean.transformation);
        assertEquals("job-99", bean.id);
        assertEquals(2048L, bean.length);
    }
}
