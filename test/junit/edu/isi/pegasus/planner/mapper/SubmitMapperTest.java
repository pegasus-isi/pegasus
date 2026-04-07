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
package edu.isi.pegasus.planner.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.jupiter.api.Test;

/** Tests for the SubmitMapper interface structure. */
public class SubmitMapperTest {

    @Test
    public void testPropertyPrefixConstant() {
        assertThat(SubmitMapper.PROPERTY_PREFIX, is("pegasus.dir.submit.mapper"));
    }

    @Test
    public void testVersionConstant() {
        assertThat(SubmitMapper.VERSION, is("1.0"));
    }

    @Test
    public void testSubmitMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(SubmitMapper.class), is(true));
    }

    @Test
    public void testGetRelativeDirMethodExists() throws NoSuchMethodException {
        assertThat(
                SubmitMapper.class.getMethod(
                        "getRelativeDir", edu.isi.pegasus.planner.classes.Job.class),
                is(notNullValue()));
    }

    @Test
    public void testGetDirMethodExists() throws NoSuchMethodException {
        assertThat(
                SubmitMapper.class.getMethod("getDir", edu.isi.pegasus.planner.classes.Job.class),
                is(notNullValue()));
    }

    @Test
    public void testSubmitMapperIsInterface() {
        assertThat(SubmitMapper.class.isInterface(), is(true));
    }

    @Test
    public void testInitializeMethodSignature() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                SubmitMapper.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        java.util.Properties.class,
                        java.io.File.class);

        assertThat(method.getReturnType(), is(void.class));
    }

    @Test
    public void testGetRelativeDirReturnsFile() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                SubmitMapper.class.getMethod(
                        "getRelativeDir", edu.isi.pegasus.planner.classes.Job.class);

        assertThat(method.getReturnType(), is(java.io.File.class));
    }

    @Test
    public void testGetDirReturnsFile() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                SubmitMapper.class.getMethod("getDir", edu.isi.pegasus.planner.classes.Job.class);

        assertThat(method.getReturnType(), is(java.io.File.class));
    }
}
