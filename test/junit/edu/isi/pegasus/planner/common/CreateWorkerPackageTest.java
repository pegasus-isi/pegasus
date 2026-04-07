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
package edu.isi.pegasus.planner.common;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.gridstart.PegasusLite;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Structural tests for CreateWorkerPackage via reflection.
 *
 * @author Rajiv Mayani
 */
public class CreateWorkerPackageTest {

    @TempDir Path mTempDir;

    private PegasusBag createBagWithTempPegasusHome(Path pegasusHome, Path submitDir)
            throws Exception {
        String original = System.getProperty("pegasus.home");

        edu.isi.pegasus.planner.common.PegasusProperties props =
                edu.isi.pegasus.planner.common.PegasusProperties.nonSingletonInstance();
        File sharedDir = pegasusHome.resolve("share").resolve("pegasus").toFile();
        sharedDir.mkdirs();

        Object commonProps = ReflectionTestUtils.getField(props, "mProps");
        ReflectionTestUtils.setField(commonProps, "m_sharedStateDir", sharedDir);

        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir.toString());

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);

        return bag;
    }

    @Test
    public void testCreateWorkerPackageIsConcreteClass() {
        assertThat(Modifier.isAbstract(CreateWorkerPackage.class.getModifiers()), is(false));
    }

    @Test
    public void testCreateWorkerPackageIsNotInterface() {
        assertThat(CreateWorkerPackage.class.isInterface(), is(false));
    }

    @Test
    public void testHasConstructorWithPegasusBag() throws NoSuchMethodException {
        Constructor<?> c =
                CreateWorkerPackage.class.getConstructor(
                        edu.isi.pegasus.planner.classes.PegasusBag.class);
        assertThat(c, notNullValue());
    }

    @Test
    public void testHasCopyMethod() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertThat(copyMethod, notNullValue());
    }

    @Test
    public void testHasCopyMethodWithFileArg() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy", java.io.File.class);
        assertThat(copyMethod, notNullValue());
    }

    @Test
    public void testCopyMethodReturnsFile() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertThat((Object) copyMethod.getReturnType(), is((Object) java.io.File.class));
    }

    @Test
    public void testCopyMethodWithFileArgReturnsFile() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy", java.io.File.class);
        assertThat((Object) copyMethod.getReturnType(), is((Object) java.io.File.class));
    }

    @Test
    public void testCopyMethodIsPublic() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertThat(Modifier.isPublic(copyMethod.getModifiers()), is(true));
    }

    @Test
    public void testCopyThrowsWhenPlannerOptionsMissing() {
        PegasusBag bag = new PegasusBag();
        bag.add(
                PegasusBag.PEGASUS_PROPERTIES,
                edu.isi.pegasus.planner.common.PegasusProperties.nonSingletonInstance());

        RuntimeException e =
                assertThrows(RuntimeException.class, () -> new CreateWorkerPackage(bag).copy());

        assertThat(e.getMessage(), containsString("No planner options specified"));
    }

    @Test
    public void testCopyCopiesWorkerPackageToSubmitDirectory() throws Exception {
        Path pegasusHome = Files.createDirectory(mTempDir.resolve("pegasus-home"));
        Path submitDir = Files.createDirectory(mTempDir.resolve("submit"));
        PegasusBag bag = createBagWithTempPegasusHome(pegasusHome, submitDir);

        File sharedDir = bag.getPegasusProperties().getSharedDir();
        Path workerPackagesDir = sharedDir.toPath().resolve("worker-packages");
        Files.createDirectories(workerPackagesDir);

        Version version = new Version();
        String basename =
                "pegasus-worker-" + version.getVersion() + "-" + version.getPlatform() + ".tar.gz";
        Path source = workerPackagesDir.resolve(basename);
        Files.write(source, "worker-package".getBytes(StandardCharsets.UTF_8));

        File copied = new CreateWorkerPackage(bag).copy();

        assertThat(
                copied.getAbsolutePath(),
                is(submitDir.resolve(basename).toFile().getAbsolutePath()));
        assertThat(copied.exists(), is(true));
        assertThat(Files.readString(copied.toPath(), StandardCharsets.UTF_8), is("worker-package"));
    }

    @Test
    public void testCopyPegasusLiteCommonCopiesScriptToSubmitDirectory() throws Exception {
        Path pegasusHome = Files.createDirectory(mTempDir.resolve("pegasus-home-common"));
        Path submitDir = Files.createDirectory(mTempDir.resolve("submit-common"));
        PegasusBag bag = createBagWithTempPegasusHome(pegasusHome, submitDir);

        File sharedDir = bag.getPegasusProperties().getSharedDir();
        Path shDir = sharedDir.toPath().resolve("sh");
        Files.createDirectories(shDir);
        Path source = shDir.resolve(PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME);
        Files.write(source, "echo common".getBytes(StandardCharsets.UTF_8));

        File copied = new CreateWorkerPackage(bag).copyPegasusLiteCommon();

        assertThat(
                copied.getAbsolutePath(),
                is(
                        submitDir
                                .resolve(PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME)
                                .toFile()
                                .getAbsolutePath()));
        assertThat(copied.exists(), is(true));
        assertThat(Files.readString(copied.toPath(), StandardCharsets.UTF_8), is("echo common"));
    }
}
