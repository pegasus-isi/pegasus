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
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the StagingMapperFactory class constants and structure. */
public class StagingMapperFactoryTest {

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                StagingMapperFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.mapper.staging"));
    }

    @Test
    public void testHashedStagingMapperConstant() {
        assertThat(StagingMapperFactory.HASHED_STAGING_MAPPER, is("Hashed"));
    }

    @Test
    public void testFlatStagingMapperConstant() {
        assertThat(StagingMapperFactory.FLAT_STAGING_MAPPER, is("Flat"));
    }

    @Test
    public void testFactoryClassIsPublic() {
        int modifiers = StagingMapperFactory.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testLoadInstanceMethodExists() throws NoSuchMethodException {
        assertThat(
                StagingMapperFactory.class.getMethod(
                        "loadInstance", edu.isi.pegasus.planner.classes.PegasusBag.class),
                is(notNullValue()));
    }

    @Test
    public void testLoadInstanceRejectsMissingProperties() {
        PegasusBag bag = new PegasusBag();

        StagingMapperFactoryException exception =
                assertThrows(
                        StagingMapperFactoryException.class,
                        () -> StagingMapperFactory.loadInstance(bag));

        assertThat(exception.getMessage(), is("Instantiating Staging Mapper "));
        assertThat(exception.getClassname(), is(org.hamcrest.Matchers.nullValue()));
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause().getMessage(), containsString("Invalid properties passed"));
    }

    @Test
    public void testLoadInstanceWithExplicitClassInitializesMapper() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(StagingMapper.PROPERTY_PREFIX, TrackingStagingMapper.class.getName());
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        StagingMapper mapper = StagingMapperFactory.loadInstance(bag);

        assertThat(mapper instanceof TrackingStagingMapper, is(true));
        TrackingStagingMapper tracking = (TrackingStagingMapper) mapper;
        assertThat(tracking.initializedBag, is(sameInstance(bag)));
        assertThat(tracking.initializedProperties, is(notNullValue()));
    }

    public static class TrackingStagingMapper implements StagingMapper {
        PegasusBag initializedBag;
        Properties initializedProperties;

        @Override
        public String description() {
            return "tracking";
        }

        @Override
        public void initialize(PegasusBag bag, Properties properties) {
            initializedBag = bag;
            initializedProperties = properties;
        }

        @Override
        public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn) {
            return new File(lfn);
        }

        @Override
        public File getRelativeDirectory(String site, String lfn) {
            return new File(lfn);
        }

        @Override
        public String map(
                Job job,
                File addOn,
                SiteCatalogEntry site,
                FileServer.OPERATION operation,
                String lfn)
                throws MapperException {
            return lfn;
        }
    }
}
