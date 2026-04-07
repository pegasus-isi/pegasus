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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.gridstart.container.impl.Docker;
import edu.isi.pegasus.planner.code.gridstart.container.impl.None;
import edu.isi.pegasus.planner.code.gridstart.container.impl.Shifter;
import edu.isi.pegasus.planner.code.gridstart.container.impl.Singularity;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for the ContainerShellWrapper interface. */
public class ContainerShellWrapperTest {

    @Test
    public void testVersionConstant() {
        assertThat(ContainerShellWrapper.VERSION, is("1.1"));
    }

    @Test
    public void testVersionConstantNotNull() {
        assertThat(ContainerShellWrapper.VERSION, notNullValue());
    }

    @Test
    public void testNoneImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(None.class), is(true));
    }

    @Test
    public void testDockerImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(Docker.class), is(true));
    }

    @Test
    public void testSingularityImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(Singularity.class), is(true));
    }

    @Test
    public void testShifterImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(Shifter.class), is(true));
    }

    @Test
    public void testContainerShellWrapperIsInterface() {
        assertThat(ContainerShellWrapper.class.isInterface(), is(true));
    }

    @Test
    public void testInitializeMethodSignature() throws Exception {
        Method method =
                ContainerShellWrapper.class.getMethod("initialize", PegasusBag.class, ADag.class);

        assertThat((Object) method.getReturnType(), is((Object) void.class));
    }

    @Test
    public void testWrapMethodForJobSignature() throws Exception {
        Method method = ContainerShellWrapper.class.getMethod("wrap", Job.class);

        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }

    @Test
    public void testWrapMethodForAggregatedJobSignature() throws Exception {
        Method method = ContainerShellWrapper.class.getMethod("wrap", AggregatedJob.class);

        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }

    @Test
    public void testDescribeMethodSignature() throws Exception {
        Method method = ContainerShellWrapper.class.getMethod("describe");

        assertThat((Object) method.getReturnType(), is((Object) String.class));
    }
}
