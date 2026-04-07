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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.dax.Invoke;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CallbackTest {

    @Test
    public void testInterfaceShapeAndVersionConstant() {
        assertThat(
                "Callback should be declared as an interface",
                Callback.class.isInterface(),
                is(true));
        assertThat(
                "VERSION should match the current callback API version",
                Callback.VERSION,
                is("1.6"));
        assertThat(
                "Callback should not extend any other interfaces",
                Callback.class.getInterfaces().length,
                is(0));
    }

    @Test
    public void testSelectedMethodReturnTypesAndModifiers() throws Exception {
        Method initialize = Callback.class.getMethod("initialize", PegasusBag.class, String.class);
        Method getConstructedObject = Callback.class.getMethod("getConstructedObject");
        Method cbDocument = Callback.class.getMethod("cbDocument", Map.class);
        Method cbWfInvoke = Callback.class.getMethod("cbWfInvoke", Invoke.class);
        Method cbFile = Callback.class.getMethod("cbFile", ReplicaLocation.class);
        Method cbReplicaStore = Callback.class.getMethod("cbReplicaStore", ReplicaStore.class);
        Method cbExecutable =
                Callback.class.getMethod("cbExecutable", TransformationCatalogEntry.class);
        Method cbCompoundTransformation =
                Callback.class.getMethod("cbCompoundTransformation", CompoundTransformation.class);
        Method cbTransformationStore =
                Callback.class.getMethod("cbTransformationStore", TransformationStore.class);
        Method cbSiteStore = Callback.class.getMethod("cbSiteStore", SiteStore.class);
        Method cbMetadata = Callback.class.getMethod("cbMetadata", Profile.class);
        Method cbJob = Callback.class.getMethod("cbJob", Job.class);
        Method cbParents = Callback.class.getMethod("cbParents", String.class, List.class);
        Method cbChildren = Callback.class.getMethod("cbChildren", String.class, List.class);
        Method cbDone = Callback.class.getMethod("cbDone");

        assertThat(
                "initialize should return void",
                initialize.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "getConstructedObject should return Object",
                getConstructedObject.getReturnType(),
                is(Object.class));
        assertThat(
                "cbDocument should return void",
                cbDocument.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbWfInvoke should return void",
                cbWfInvoke.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat("cbFile should return void", cbFile.getReturnType(), sameInstance(Void.TYPE));
        assertThat(
                "cbReplicaStore should return void",
                cbReplicaStore.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbExecutable should return void",
                cbExecutable.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbCompoundTransformation should return void",
                cbCompoundTransformation.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbTransformationStore should return void",
                cbTransformationStore.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbSiteStore should return void",
                cbSiteStore.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat(
                "cbMetadata should return void",
                cbMetadata.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat("cbJob should return void", cbJob.getReturnType(), sameInstance(Void.TYPE));
        assertThat(
                "cbParents should return void", cbParents.getReturnType(), sameInstance(Void.TYPE));
        assertThat(
                "cbChildren should return void",
                cbChildren.getReturnType(),
                sameInstance(Void.TYPE));
        assertThat("cbDone should return void", cbDone.getReturnType(), sameInstance(Void.TYPE));

        assertThat(
                "interface methods should be public",
                Modifier.isPublic(initialize.getModifiers()),
                is(true));
        assertThat(
                "interface methods should be abstract",
                Modifier.isAbstract(initialize.getModifiers()),
                is(true));
    }

    @Test
    public void testDeclaredMethodCount() {
        assertThat(
                "Callback should declare the expected number of methods",
                Callback.class.getDeclaredMethods().length,
                is(15));
    }
}
