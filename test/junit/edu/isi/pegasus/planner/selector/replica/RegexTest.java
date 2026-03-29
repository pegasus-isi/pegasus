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

import edu.isi.pegasus.planner.selector.ReplicaSelector;
import org.junit.jupiter.api.Test;

/** Tests for the Regex replica selector class structure. */
public class RegexTest {

    @Test
    public void testClassExtendsDefault() {
        assertTrue(Default.class.isAssignableFrom(Regex.class));
    }

    @Test
    public void testImplementsReplicaSelector() {
        assertTrue(ReplicaSelector.class.isAssignableFrom(Regex.class));
    }

    @Test
    public void testPropertyPrefixConstantExists() throws Exception {
        java.lang.reflect.Field f = Regex.class.getDeclaredField("PROPERTY_PREFIX");
        f.setAccessible(true);
        assertThat((String) f.get(null), containsString("regex"));
    }

    @Test
    public void testConstructorRequiresPegasusProperties() {
        // PegasusProperties.nonSingletonInstance() needs a Pegasus installation;
        // verify the constructor signature accepts PegasusProperties
        java.lang.reflect.Constructor<?>[] ctors = Regex.class.getDeclaredConstructors();
        assertThat(ctors.length, greaterThan(0));
        assertThat(ctors[0].getParameterTypes()[0].getSimpleName(), is("PegasusProperties"));
    }
}
