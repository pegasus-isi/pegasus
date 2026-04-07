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
package edu.isi.pegasus.planner.selector;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelector interface constants. */
public class ReplicaSelectorTest {

    @Test
    public void testVersionConstant() {
        assertThat(ReplicaSelector.VERSION, notNullValue());
        assertThat(ReplicaSelector.VERSION.isEmpty(), is(false));
    }

    @Test
    public void testVersionFormat() {
        // Version should match major.minor format
        assertThat(ReplicaSelector.VERSION.matches("\\d+\\.\\d+"), is(true));
    }

    @Test
    public void testLocalSiteHandleValue() {
        assertThat(ReplicaSelector.LOCAL_SITE_HANDLE, is("local"));
    }

    @Test
    public void testLocalSiteHandleNotEmpty() {
        assertThat(ReplicaSelector.LOCAL_SITE_HANDLE.isEmpty(), is(false));
    }

    @Test
    public void testPriorityKeyNotNull() {
        assertThat(ReplicaSelector.PRIORITY_KEY, notNullValue());
    }

    @Test
    public void testPriorityKeyValue() {
        assertThat(ReplicaSelector.PRIORITY_KEY, is("priority"));
    }

    @Test
    public void testReplicaSelectorIsInterface() {
        assertThat(ReplicaSelector.class.isInterface(), is(true));
    }

    @Test
    public void testMethodReturnTypes() throws Exception {
        assertThat(
                (Object)
                        ReplicaSelector.class
                                .getMethod(
                                        "selectAndOrderReplicas",
                                        edu.isi.pegasus.planner.classes.ReplicaLocation.class,
                                        String.class,
                                        Boolean.TYPE)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.ReplicaLocation.class));
        assertThat(
                (Object)
                        ReplicaSelector.class
                                .getMethod(
                                        "selectReplica",
                                        edu.isi.pegasus.planner.classes.ReplicaLocation.class,
                                        String.class,
                                        Boolean.TYPE)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry.class));
        assertThat(
                (Object) ReplicaSelector.class.getMethod("description").getReturnType(),
                is((Object) String.class));
    }

    @Test
    public void testReplicaSelectorDeclaresExpectedMethods() {
        assertThat(ReplicaSelector.class.getDeclaredMethods().length, is(3));
    }
}
