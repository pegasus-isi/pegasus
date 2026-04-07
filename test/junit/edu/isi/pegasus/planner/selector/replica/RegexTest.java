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
        assertThat(Default.class.isAssignableFrom(Regex.class), is(true));
    }

    @Test
    public void testImplementsReplicaSelector() {
        assertThat(ReplicaSelector.class.isAssignableFrom(Regex.class), is(true));
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

    @Test
    public void testAdditionalPrivateConstantsExist() throws Exception {
        java.lang.reflect.Field highest = Regex.class.getDeclaredField("HIGHEST_RANK_VALUE");
        java.lang.reflect.Field lowest = Regex.class.getDeclaredField("LOWEST_RANK_VALUE");
        highest.setAccessible(true);
        lowest.setAccessible(true);

        assertThat(highest.getInt(null), is(1));
        assertThat(lowest.getInt(null), is(Integer.MAX_VALUE));
    }

    @Test
    public void testPublicMethodReturnTypes() throws Exception {
        assertThat(
                Regex.class
                        .getMethod(
                                "selectAndOrderReplicas",
                                edu.isi.pegasus.planner.classes.ReplicaLocation.class,
                                String.class,
                                Boolean.TYPE)
                        .getReturnType(),
                is(edu.isi.pegasus.planner.classes.ReplicaLocation.class));
        assertThat(
                Regex.class
                        .getMethod(
                                "selectReplica",
                                edu.isi.pegasus.planner.classes.ReplicaLocation.class,
                                String.class,
                                Boolean.TYPE)
                        .getReturnType(),
                is(edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry.class));
        assertThat(Regex.class.getMethod("description").getReturnType(), is(String.class));
    }

    @Test
    public void testPrivateHelperMethodExists() throws Exception {
        assertThat(
                Regex.class
                        .getDeclaredMethod("getRegexSet", java.util.Properties.class)
                        .getReturnType(),
                is(java.util.SortedSet.class));
    }
}
