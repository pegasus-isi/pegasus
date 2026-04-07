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

/** Tests for the Mapper interface structure. */
public class MapperTest {

    @Test
    public void testMapperDescriptionMethodExists() throws NoSuchMethodException {
        assertThat(Mapper.class.getMethod("description"), is(notNullValue()));
    }

    @Test
    public void testOutputMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(OutputMapper.class), is(true));
    }

    @Test
    public void testStagingMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(StagingMapper.class), is(true));
    }

    @Test
    public void testSubmitMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(SubmitMapper.class), is(true));
    }

    @Test
    public void testMapperIsInterface() {
        assertThat(Mapper.class.isInterface(), is(true));
    }

    @Test
    public void testDescriptionMethodReturnsStringAndIsAbstract() throws NoSuchMethodException {
        java.lang.reflect.Method method = Mapper.class.getMethod("description");

        assertThat(method.getReturnType(), is(String.class));
        assertThat(method.getParameterCount(), is(0));
        assertThat(java.lang.reflect.Modifier.isPublic(method.getModifiers()), is(true));
        assertThat(java.lang.reflect.Modifier.isAbstract(method.getModifiers()), is(true));
    }

    @Test
    public void testMapperDeclaresOnlyDescriptionMethod() {
        assertThat(Mapper.class.getDeclaredMethods().length, is(1));
    }
}
