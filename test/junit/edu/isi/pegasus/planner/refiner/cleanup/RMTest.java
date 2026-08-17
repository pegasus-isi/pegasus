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
package edu.isi.pegasus.planner.refiner.cleanup;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.common.PegasusProperties;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

/** Structural tests for RM cleanup implementation. */
public class RMTest {

    @Test
    public void testImplementsCleanupImplementation() {
        assertThat(CleanupImplementation.class.isAssignableFrom(RM.class), is(true));
    }

    @Test
    public void testDefaultRmLogicalName() {
        assertThat(RM.DEFAULT_RM_LOGICAL_NAME, is("rm"));
    }

    @Test
    public void testDefaultRmLocation() {
        assertThat(RM.DEFAULT_RM_LOCATION, is("/bin/rm"));
    }

    @Test
    public void testDefaultPriorityKey() {
        assertThat(RM.DEFAULT_PRIORITY_KEY, is("1000"));
    }

    @Test
    public void testDefaultConstructor() {
        RM rm = new RM();
        assertThat(rm, notNullValue());
    }

    @Test
    public void testInitializeMethodReturnsVoid() throws Exception {
        assertThat(
                (Object)
                        RM.class
                                .getMethod(
                                        "initialize",
                                        edu.isi.pegasus.planner.classes.PegasusBag.class)
                                .getReturnType(),
                is((Object) Void.TYPE));
    }

    @Test
    public void testCreateCleanupJobOverloadsReturnJob() throws Exception {
        assertThat(
                (Object)
                        RM.class
                                .getMethod(
                                        "createCleanupJob",
                                        String.class,
                                        java.util.List.class,
                                        edu.isi.pegasus.planner.classes.Job.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
        assertThat(
                (Object)
                        RM.class
                                .getMethod(
                                        "createCleanupJob",
                                        String.class,
                                        java.util.List.class,
                                        edu.isi.pegasus.planner.classes.Job.class,
                                        String.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testGetTCEntryMethodExists() throws Exception {
        assertThat(
                (Object) RM.class.getDeclaredMethod("getTCEntry", String.class).getReturnType(),
                is(
                        (Object)
                                edu.isi.pegasus.planner.catalog.transformation
                                        .TransformationCatalogEntry.class));
    }

    /**
     * GH-233 a directory's physical artifact at the staging site is always &lt;lfn&gt;.tar.gz, not
     * the plain lfn (see StageIn/StageOut) - the generated rm command must be told to remove that,
     * or it silently does nothing and the tarball is never cleaned up.
     */
    @Test
    public void testCreateCleanupJobAppendsTarGzSuffixForDirectory() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        SiteStore store = new SiteStore();
        store.addEntry(new SiteCatalogEntry("compute"));

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.SITE_STORE, store);

        RM rm = new RM();
        rm.initialize(bag);

        Job job = new Job();
        job.setSiteHandle("compute");
        job.setStagingSiteHandle("compute");

        PegasusFile directoryFile = new PegasusFile("out");
        directoryFile.setForDirectory();
        PegasusFile plainFile = new PegasusFile("f.out");

        List<PegasusFile> files = new LinkedList();
        files.add(directoryFile);
        files.add(plainFile);

        Job cleanupJob = rm.createCleanupJob("clean_up_1", files, job);

        assertThat(cleanupJob.getArguments(), containsString("out.tar.gz"));
        assertThat(cleanupJob.getArguments(), containsString("f.out"));
        assertThat(cleanupJob.getArguments(), not(containsString("f.out.tar.gz")));
    }

    @Test
    public void testDefaultTCEntryMethodExists() throws Exception {
        java.lang.reflect.Method method = RM.class.getDeclaredMethod("defaultTCEntry");
        assertThat(
                (Object) method.getReturnType(),
                is(
                        (Object)
                                edu.isi.pegasus.planner.catalog.transformation
                                        .TransformationCatalogEntry.class));
        assertThat(java.lang.reflect.Modifier.isStatic(method.getModifiers()), is(true));
    }
}
