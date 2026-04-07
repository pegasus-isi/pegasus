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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the OutputMapperFactory class constants and structure. */
public class OutputMapperFactoryTest {

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                OutputMapperFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.mapper.output"));
    }

    @Test
    public void testPropertyKeyConstant() {
        assertThat(OutputMapperFactory.PROPERTY_KEY, is("pegasus.dir.storage.mapper"));
    }

    @Test
    public void testDefaultOutputMapperImplementationConstant() {
        assertThat(OutputMapperFactory.DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION, is("Flat"));
    }

    @Test
    public void testHashedOutputMapperImplementationConstant() {
        assertThat(OutputMapperFactory.HASHED_OUTPUT_MAPPER_IMPLEMENTATION, is("Hashed"));
    }

    @Test
    public void testFactoryClassIsPublic() {
        int modifiers = OutputMapperFactory.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testLoadInstanceRejectsNullPropertiesInBag() {
        PegasusBag bag = new PegasusBag();
        ADag dag = new ADag();

        OutputMapperFactoryException exception =
                assertThrows(
                        OutputMapperFactoryException.class,
                        () -> OutputMapperFactory.loadInstance(dag, bag));

        assertThat(exception.getMessage(), is("Null Properties passed in the bag "));
    }

    @Test
    public void testLoadInstanceWithNullDagWrapsFailure() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        OutputMapperFactoryException exception =
                assertThrows(
                        OutputMapperFactoryException.class,
                        () ->
                                OutputMapperFactory.loadInstance(
                                        TrackingOutputMapper.class.getName(), bag, null));

        assertThat(exception.getMessage(), is("Instantiating Output Mapper"));
        assertThat(exception.getClassname(), is(TrackingOutputMapper.class.getName()));
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause().getMessage(), containsString("Invalid workflow passed"));
    }

    @Test
    public void testLoadInstanceWithExplicitClassInitializesMapper() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        ADag dag = new ADag();

        OutputMapper mapper =
                OutputMapperFactory.loadInstance(TrackingOutputMapper.class.getName(), bag, dag);

        assertThat(mapper instanceof TrackingOutputMapper, is(true));
        TrackingOutputMapper tracking = (TrackingOutputMapper) mapper;
        assertThat(tracking.initializedBag, is(sameInstance(bag)));
        assertThat(tracking.initializedDag, is(sameInstance(dag)));
    }

    public static class TrackingOutputMapper implements OutputMapper {
        PegasusBag initializedBag;
        ADag initializedDag;

        @Override
        public String description() {
            return "tracking";
        }

        @Override
        public void initialize(PegasusBag bag, ADag workflow) throws MapperException {
            initializedBag = bag;
            initializedDag = workflow;
        }

        @Override
        public NameValue<String, String> map(
                String lfn, String site, FileServer.OPERATION operation) throws MapperException {
            return new NameValue<String, String>(site, lfn);
        }

        @Override
        public NameValue<String, String> map(
                String lfn, String site, FileServer.OPERATION operation, boolean existing)
                throws MapperException {
            return new NameValue<String, String>(site, lfn);
        }

        @Override
        public List<NameValue<String, String>> mapAll(
                String lfn, String site, FileServer.OPERATION operation) throws MapperException {
            return Collections.singletonList(new NameValue<String, String>(site, lfn));
        }
    }
}
