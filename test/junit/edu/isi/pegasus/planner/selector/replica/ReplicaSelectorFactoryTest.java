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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelectorFactory. */
public class ReplicaSelectorFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.selector.replica",
                ReplicaSelectorFactory.DEFAULT_PACKAGE_NAME,
                "Default package name should match");
    }

    @Test
    public void testDefaultReplicaSelectorName() {
        assertEquals(
                "Default",
                ReplicaSelectorFactory.DEFAULT_REPLICA_SELECTOR,
                "Default replica selector class name should be 'Default'");
    }

    @Test
    public void testLoadDefaultSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props);
        assertNotNull(rs, "Loaded replica selector should not be null");
        assertInstanceOf(Default.class, rs, "Default selector should be instance of Default class");
    }

    @Test
    public void testLoadLocalSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props, "Local");
        assertNotNull(rs, "Loaded Local selector should not be null");
        assertInstanceOf(Local.class, rs, "Should be instance of Local class");
    }

    @Test
    public void testLoadRegexSelectorClassExists() {
        // Regex constructor fails in environments without Pegasus installation.
        // Verify the class exists and is a ReplicaSelector without instantiating it.
        assertTrue(ReplicaSelector.class.isAssignableFrom(Regex.class));
    }

    @Test
    public void testLoadRestrictedSelector() throws ReplicaSelectorFactoryException {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        ReplicaSelector rs = ReplicaSelectorFactory.loadInstance(props, "Restricted");
        assertNotNull(rs, "Loaded Restricted selector should not be null");
        assertInstanceOf(Restricted.class, rs, "Should be instance of Restricted class");
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
        assertNotNull(rs, "Selector loaded via property should not be null");
        assertInstanceOf(Local.class, rs, "Should be Local selector as specified in properties");
    }
}
