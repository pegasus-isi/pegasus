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
package edu.isi.pegasus.planner.code.gridstart.container;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Condor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the ContainerShellWrapperFactory class. */
public class ContainerShellWrapperFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertThat(
                ContainerShellWrapperFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.code.gridstart.container.impl"));
    }

    @Test
    public void testDockerShellWrapperClass() {
        assertThat(ContainerShellWrapperFactory.DOCKER_SHELL_WRAPPER_CLASS, is("Docker"));
    }

    @Test
    public void testSingularityShellWrapperClass() {
        assertThat(ContainerShellWrapperFactory.SINGULARITY_SHELL_WRAPPER_CLASS, is("Singularity"));
    }

    @Test
    public void testShifterShellWrapperClass() {
        assertThat(ContainerShellWrapperFactory.SHIFTER_SHELL_WRAPPER_CLASS, is("Shifter"));
    }

    @Test
    public void testNoShellWrapperClass() {
        assertThat(ContainerShellWrapperFactory.NO_SHELL_WRAPPER_CLASS, is("None"));
    }

    @Test
    public void testContainerShortNamesNotNull() {
        assertThat(ContainerShellWrapperFactory.CONTAINER_SHORT_NAMES, notNullValue());
        assertThat(ContainerShellWrapperFactory.CONTAINER_SHORT_NAMES.length > 0, is(true));
    }

    @Test
    public void testContainerImplementingClassesNotNull() {
        assertThat(ContainerShellWrapperFactory.CONTAINER_IMPLEMENTING_CLASSES, notNullValue());
        assertThat(
                ContainerShellWrapperFactory.CONTAINER_IMPLEMENTING_CLASSES.length > 0, is(true));
    }

    @Test
    public void testContainerShortNamesAlignWithImplementingClasses() {
        assertThat(
                ContainerShellWrapperFactory.CONTAINER_SHORT_NAMES.length,
                is(ContainerShellWrapperFactory.CONTAINER_IMPLEMENTING_CLASSES.length));
        assertThat(
                ContainerShellWrapperFactory.CONTAINER_SHORT_NAMES,
                arrayContaining("docker", "singularity", "shifter", "none"));
    }

    @Test
    public void testRegisterContainerShellWrapperStoresLowerCaseKey() throws Exception {
        ContainerShellWrapperFactory factory = new ContainerShellWrapperFactory();
        Method register =
                ContainerShellWrapperFactory.class.getDeclaredMethod(
                        "registerContainerShellWrapper", String.class, ContainerShellWrapper.class);
        register.setAccessible(true);

        ContainerShellWrapper wrapper =
                new ContainerShellWrapper() {
                    @Override
                    public void initialize(
                            edu.isi.pegasus.planner.classes.PegasusBag bag,
                            edu.isi.pegasus.planner.classes.ADag dag) {}

                    @Override
                    public String wrap(Job job) {
                        return "";
                    }

                    @Override
                    public String wrap(edu.isi.pegasus.planner.classes.AggregatedJob job) {
                        return "";
                    }

                    @Override
                    public String describe() {
                        return "stub";
                    }
                };

        register.invoke(factory, "DoCkEr", wrapper);

        Field field =
                ContainerShellWrapperFactory.class.getDeclaredField(
                        "mContainerWrapperImplementationTable");
        field.setAccessible(true);
        Map<String, ContainerShellWrapper> table =
                (Map<String, ContainerShellWrapper>) field.get(factory);

        assertThat(table.get("docker"), sameInstance(wrapper));
    }

    @Test
    public void testLoadInstanceRejectsDockerContainerUniverseExecution() {
        ContainerShellWrapperFactory factory = new ContainerShellWrapperFactory();
        Job job = new Job();
        Container container = new Container("c");
        container.setType(Container.TYPE.docker);
        job.setContainer(container);
        job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.CONTAINER_UNIVERSE);

        ContainerShellWrapperFactoryException e =
                assertThrows(
                        ContainerShellWrapperFactoryException.class,
                        () -> factory.loadInstance(job));

        assertThat(e, notNullValue());
    }

    @Test
    public void testPrivateLoadInstanceWrapsDynamicLoadingFailure() throws Exception {
        Method load =
                ContainerShellWrapperFactory.class.getDeclaredMethod(
                        "loadInstance",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        edu.isi.pegasus.planner.classes.ADag.class,
                        String.class);
        load.setAccessible(true);

        InvocationTargetException e =
                assertThrows(
                        InvocationTargetException.class,
                        () -> load.invoke(null, null, null, "DefinitelyMissingWrapper"));

        assertThat(e.getCause(), instanceOf(ContainerShellWrapperFactoryException.class));
        assertThat(
                e.getCause().getMessage(), containsString("Instantiating Container Shell Wrapper"));
    }
}
