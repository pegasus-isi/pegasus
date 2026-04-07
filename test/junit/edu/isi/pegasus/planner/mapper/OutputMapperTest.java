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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

/** Tests for the OutputMapper interface structure. */
public class OutputMapperTest {

    @Test
    public void testVersionConstant() {
        assertThat(OutputMapper.VERSION, is("1.1"));
    }

    @Test
    public void testOutputMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(OutputMapper.class), is(true));
    }

    @Test
    public void testMapMethodExists() throws NoSuchMethodException {
        assertThat(
                OutputMapper.class.getMethod(
                        "map",
                        String.class,
                        String.class,
                        edu.isi.pegasus.planner.catalog.site.classes.FileServer.OPERATION.class),
                is(notNullValue()));
    }

    @Test
    public void testInitializeMethodExists() throws NoSuchMethodException {
        assertThat(
                OutputMapper.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        edu.isi.pegasus.planner.classes.ADag.class),
                is(notNullValue()));
    }

    @Test
    public void testOutputMapperIsInterface() {
        assertThat(OutputMapper.class.isInterface(), is(true));
    }

    @Test
    public void testOverloadedMapMethodExistsAndThrowsMapperException()
            throws NoSuchMethodException {
        java.lang.reflect.Method method =
                OutputMapper.class.getMethod(
                        "map",
                        String.class,
                        String.class,
                        edu.isi.pegasus.planner.catalog.site.classes.FileServer.OPERATION.class,
                        boolean.class);

        assertThat(method.getReturnType(), is(edu.isi.pegasus.planner.classes.NameValue.class));
        assertArrayEquals(new Class<?>[] {MapperException.class}, method.getExceptionTypes());
    }

    @Test
    public void testMapAllMethodExistsAndThrowsMapperException() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                OutputMapper.class.getMethod(
                        "mapAll",
                        String.class,
                        String.class,
                        edu.isi.pegasus.planner.catalog.site.classes.FileServer.OPERATION.class);

        assertThat(method.getReturnType(), is(java.util.List.class));
        assertArrayEquals(new Class<?>[] {MapperException.class}, method.getExceptionTypes());
    }

    @Test
    public void testInitializeMethodThrowsMapperException() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                OutputMapper.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        edu.isi.pegasus.planner.classes.ADag.class);

        assertThat(method.getReturnType(), is(void.class));
        assertArrayEquals(new Class<?>[] {MapperException.class}, method.getExceptionTypes());
    }
}
