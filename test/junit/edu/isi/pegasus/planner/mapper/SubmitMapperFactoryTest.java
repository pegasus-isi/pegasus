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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the SubmitMapperFactory class constants and structure. */
public class SubmitMapperFactoryTest {

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                SubmitMapperFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.mapper.submit"));
    }

    @Test
    public void testDefaultCreatorConstant() {
        assertThat(SubmitMapperFactory.DEFAULT_CREATOR, is("Hashed"));
    }

    @Test
    public void testFactoryClassIsPublic() {
        int modifiers = SubmitMapperFactory.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testLoadInstanceMethodExists() throws NoSuchMethodException {
        assertThat(
                SubmitMapperFactory.class.getMethod(
                        "loadInstance",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        java.io.File.class),
                is(notNullValue()));
    }

    @Test
    public void testLoadInstanceRejectsMissingProperties() {
        PegasusBag bag = new PegasusBag();

        SubmitMapperFactoryException exception =
                assertThrows(
                        SubmitMapperFactoryException.class,
                        () -> SubmitMapperFactory.loadInstance(bag, new File(".")));

        assertThat(exception.getMessage(), is("Instantiating SubmitMapper "));
        assertThat(exception.getClassname(), is(org.hamcrest.Matchers.nullValue()));
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause().getMessage(), containsString("Invalid properties passed"));
    }

    @Test
    public void testLoadInstanceWithExplicitClassInitializesMapper() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(SubmitMapper.PROPERTY_PREFIX, TrackingSubmitMapper.class.getName());
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        File base = new File("submit-base");

        SubmitMapper mapper = SubmitMapperFactory.loadInstance(bag, base);

        assertThat(mapper instanceof TrackingSubmitMapper, is(true));
        TrackingSubmitMapper tracking = (TrackingSubmitMapper) mapper;
        assertThat(tracking.initializedBag, is(sameInstance(bag)));
        assertThat(tracking.initializedProperties, is(notNullValue()));
        assertThat(tracking.initializedBase, is(base));
    }

    public static class TrackingSubmitMapper implements SubmitMapper {
        PegasusBag initializedBag;
        Properties initializedProperties;
        File initializedBase;

        @Override
        public String description() {
            return "tracking";
        }

        @Override
        public void initialize(PegasusBag bag, Properties properties, File base) {
            initializedBag = bag;
            initializedProperties = properties;
            initializedBase = base;
        }

        @Override
        public File getRelativeDir(Job job) {
            return new File("relative");
        }

        @Override
        public File getDir(Job job) {
            return new File(initializedBase, "dir");
        }
    }
}
