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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FileDataBean} data bean class. */
public class FileDataBeanTest {

    private FileDataBean bean;

    @BeforeEach
    public void setUp() {
        bean = new FileDataBean();
    }

    @Test
    public void testDefaultFieldsAreNull() {
        assertThat(bean.transformation, nullValue());
        assertThat(bean.id, nullValue());
        assertThat(bean.type, nullValue());
        assertThat(bean.filename, nullValue());
        assertThat(bean.length, is(0L));
    }

    @Test
    public void testSetTransformation() {
        bean.setTransformation("blackdiamond::preprocess");
        assertThat(bean.transformation, is("blackdiamond::preprocess"));
    }

    @Test
    public void testSetId() {
        bean.setId("job-001");
        assertThat(bean.id, is("job-001"));
    }

    @Test
    public void testSetType() {
        bean.setType("input");
        assertThat(bean.type, is("input"));
    }

    @Test
    public void testSetFilename() {
        bean.setFilename("input.txt");
        assertThat(bean.filename, is("input.txt"));
    }

    @Test
    public void testSetLength() {
        bean.setLength(1048576L);
        assertThat(bean.length, is(1048576L));
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
        assertThat(bean.transformation, is("mypfn"));
        assertThat(bean.id, is("job-99"));
        assertThat(bean.length, is(2048L));
    }

    @Test
    public void testToStringExactFormat() {
        bean.setTransformation("tx");
        bean.setId("id");
        bean.setType("type");
        bean.setFilename("name");
        bean.setLength(7L);

        assertThat(
                bean.toString(),
                is(
                        "FileDataBean{transformation='tx', id='id', type='type', filename='name', length=7}"));
    }

    @Test
    public void testToStringRendersNullFields() {
        assertThat(
                bean.toString(),
                is(
                        "FileDataBean{transformation='null', id='null', type='null', filename='null', length=0}"));
    }

    @Test
    public void testDeclaresExpectedPublicFields() throws Exception {
        assertThat(
                java.lang.reflect.Modifier.isPublic(
                        FileDataBean.class.getDeclaredField("transformation").getModifiers()),
                is(true));
        assertThat(
                java.lang.reflect.Modifier.isPublic(
                        FileDataBean.class.getDeclaredField("id").getModifiers()),
                is(true));
        assertThat(
                java.lang.reflect.Modifier.isPublic(
                        FileDataBean.class.getDeclaredField("type").getModifiers()),
                is(true));
        assertThat(
                java.lang.reflect.Modifier.isPublic(
                        FileDataBean.class.getDeclaredField("filename").getModifiers()),
                is(true));
        assertThat(
                (Object) FileDataBean.class.getDeclaredField("length").getType(),
                is((Object) Long.TYPE));
    }
}
