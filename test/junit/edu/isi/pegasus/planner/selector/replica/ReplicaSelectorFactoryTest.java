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
package edu.isi.pegasus.planner.selector.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelectorFactory. */
public class ReplicaSelectorFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertThat(
                ReplicaSelectorFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.selector.replica"));
    }

    @Test
    public void testDefaultReplicaSelectorName() {
        assertThat(ReplicaSelectorFactory.DEFAULT_REPLICA_SELECTOR, is("Default"));
    }

    @Test
    public void testLoadDefaultSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props);
        assertThat(rs, notNullValue());
        assertThat(rs, instanceOf(Default.class));
    }

    @Test
    public void testLoadLocalSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props, "Local");
        assertThat(rs, notNullValue());
        assertThat(rs, instanceOf(Local.class));
    }

    @Test
    public void testLoadRegexSelectorClassExists() {
        // Regex constructor fails in environments without Pegasus installation.
        // Verify the class exists and is a ReplicaSelector without instantiating it.
        assertThat(ReplicaSelector.class.isAssignableFrom(Regex.class), is(true));
    }

    @Test
    public void testLoadRestrictedSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props, "Restricted");
        assertThat(rs, notNullValue());
        assertThat(rs, instanceOf(Restricted.class));
    }

    @Test
    public void testLoadWithNullPropertiesThrows() {
        assertThrows(
                ReplicaSelectorFactoryException.class,
                () -> ReplicaSelectorFactory.loadInstance(null),
                "Loading with null properties should throw ReplicaSelectorFactoryException");
    }

    @Test
    public void testLoadWithInvalidClassThrows() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        assertThrows(
                ReplicaSelectorFactoryException.class,
                () -> ReplicaSelectorFactory.loadInstance(props, "NonExistentSelector"),
                "Loading non-existent class should throw ReplicaSelectorFactoryException");
    }

    @Test
    public void testLoadSelectorWithPropertySet() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica", "Local");
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props);
        assertThat(rs, notNullValue());
        assertThat(rs, instanceOf(Local.class));
    }

    @Test
    public void testLoadWithFullyQualifiedClassName() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs =
                ReplicaSelectorFactory.loadInstance(
                        props, "edu.isi.pegasus.planner.selector.replica.Local");
        assertThat(rs, instanceOf(Local.class));
    }

    @Test
    public void testLoadWithNullClassNameThrows() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        assertThrows(
                ReplicaSelectorFactoryException.class,
                () -> ReplicaSelectorFactory.loadInstance(props, null));
    }

    @Test
    public void testLoadDefaultsWhenPropertyIsBlank() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.selector.replica", " ");
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props);
        assertThat(rs, instanceOf(Default.class));
    }
}
