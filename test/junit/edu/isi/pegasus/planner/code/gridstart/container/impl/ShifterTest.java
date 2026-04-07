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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for the Shifter container shell wrapper class. */
public class ShifterTest {

    @Test
    public void testShifterImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(Shifter.class), is(true));
    }

    @Test
    public void testContainerWorkingDirectory() {
        assertThat(Shifter.CONTAINER_WORKING_DIRECTORY, is("/scratch"));
    }

    @Test
    public void testContainerWorkingDirectoryNotNull() {
        assertThat(Shifter.CONTAINER_WORKING_DIRECTORY, notNullValue());
    }

    @Test
    public void testShifterClassExists() {
        assertThat(Shifter.class, notNullValue());
    }

    @Test
    public void testDescribe() {
        assertThat(new Shifter().describe(), is("Shifter@NERSC"));
    }

    @Test
    public void testContainerInitUsesContainerLFN() {
        Shifter shifter = new Shifter();
        Job job = new Job();
        Container container = new Container("ignored");
        container.setType(Container.TYPE.shifter);
        container.setImageURL("docker:///library/alpine:latest");
        job.setContainer(container);

        assertThat(
                shifter.containerInit(job).toString(),
                is("shifter_init docker:library/alpine:latest"));
    }

    @Test
    public void testContainerRunIncludesVolumesWorkdirExtraArgsAndLaunchScript() {
        Shifter shifter = new Shifter();
        Job job = new Job();
        job.setName("jobA");
        job.setJobType(Job.CLEANUP_JOB);
        job.vdsNS.construct(Pegasus.CONTAINER_ARGUMENTS_KEY, "--verbose");

        Container container = new Container("ignored");
        container.setType(Container.TYPE.shifter);
        container.setImageURL("shifter:///opt/images/app.sqsh");
        container.addMountPoint("/host/data:/container/data:ro");
        job.setContainer(container);

        String result = shifter.containerRun(job).toString();

        assertThat(result, startsWith("shifter --image opt/images/app.sqsh "));
        assertThat(result, containsString("--volume /host/data:/container/data:ro;$PWD:/scratch "));
        assertThat(result, containsString("--workdir=/scratch "));
        assertThat(result, containsString("--verbose "));
        assertThat(result, endsWith("./jobA-cont.sh "));
    }

    @Test
    public void testConstructJobEnvironmentInContainerUsesContainerEnvProfiles() {
        Shifter shifter = new Shifter();
        Job job = new Job();
        Container container = new Container("c");
        container.addProfile(new Profile(ENV.NAMESPACE_NAME, "FOO", "bar"));
        container.addProfile(
                new Profile(ENV.NAMESPACE_NAME, "CONDOR_JOBID", "$(cluster).$(process)"));
        job.setContainer(container);

        String result = shifter.constructJobEnvironmentInContainer(job);

        assertThat(result, containsString("# setting environment variables for job"));
        assertThat(result, containsString("FOO=\"bar\""));
        assertThat(result, containsString("export FOO"));
        assertThat(result, containsString("CONDOR_JOBID==$CONDOR_JOBID"));
        assertThat(result, containsString("export CONDOR_JOBID"));
    }

    @Test
    public void testComputeShifterImageNameDropsLeadingSlashAndPrefixesProtocol() throws Exception {
        Shifter shifter = new Shifter();
        Container container = new Container("c");
        container.setType(Container.TYPE.shifter);
        container.setImageURL("docker:///library/alpine:3.19");

        Method method = Shifter.class.getDeclaredMethod("computeShifterImageName", Container.class);
        method.setAccessible(true);

        assertThat(method.invoke(shifter, container), is((Object) "docker:library/alpine:3.19"));
    }
}
