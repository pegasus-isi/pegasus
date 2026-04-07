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

/** Tests for the StagingMapper interface structure. */
public class StagingMapperTest {

    @Test
    public void testPropertyPrefixConstant() {
        assertThat(StagingMapper.PROPERTY_PREFIX, is("pegasus.dir.staging.mapper"));
    }

    @Test
    public void testVersionConstant() {
        assertThat(StagingMapper.VERSION, is("1.0"));
    }

    @Test
    public void testStagingMapperExtendsMapper() {
        assertThat(Mapper.class.isAssignableFrom(StagingMapper.class), is(true));
    }

    @Test
    public void testInitializeMethodExists() throws NoSuchMethodException {
        assertThat(
                StagingMapper.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        java.util.Properties.class),
                is(notNullValue()));
    }

    @Test
    public void testStagingMapperIsInterface() {
        assertThat(StagingMapper.class.isInterface(), is(true));
    }

    @Test
    public void testMapToRelativeDirectoryMethodSignature() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                StagingMapper.class.getMethod(
                        "mapToRelativeDirectory",
                        edu.isi.pegasus.planner.classes.Job.class,
                        edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry.class,
                        String.class);

        assertThat(method.getReturnType(), is(java.io.File.class));
    }

    @Test
    public void testGetRelativeDirectoryMethodSignature() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                StagingMapper.class.getMethod("getRelativeDirectory", String.class, String.class);

        assertThat(method.getReturnType(), is(java.io.File.class));
    }

    @Test
    public void testMapMethodThrowsMapperException() throws NoSuchMethodException {
        java.lang.reflect.Method method =
                StagingMapper.class.getMethod(
                        "map",
                        edu.isi.pegasus.planner.classes.Job.class,
                        java.io.File.class,
                        edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry.class,
                        edu.isi.pegasus.planner.catalog.site.classes.FileServer.OPERATION.class,
                        String.class);

        assertThat(method.getReturnType(), is(String.class));
        assertArrayEquals(new Class<?>[] {MapperException.class}, method.getExceptionTypes());
    }
}
