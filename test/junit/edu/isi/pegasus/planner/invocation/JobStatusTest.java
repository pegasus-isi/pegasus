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
package edu.isi.pegasus.planner.invocation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for JobStatus abstract class structure. */
public class JobStatusTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(JobStatus.class), is(true));
    }

    @Test
    public void testJobStatusIsAbstract() {
        assertThat(Modifier.isAbstract(JobStatus.class.getModifiers()), is(true));
    }

    @Test
    public void testJobStatusRegularIsConcreteSubclass() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusRegular.class), is(true));
        assertThat(Modifier.isAbstract(JobStatusRegular.class.getModifiers()), is(false));
    }

    @Test
    public void testJobStatusFailureIsConcreteSubclass() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusFailure.class), is(true));
        assertThat(Modifier.isAbstract(JobStatusFailure.class.getModifiers()), is(false));
    }

    @Test
    public void testJobStatusSignalIsConcreteSubclass() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusSignal.class), is(true));
        assertThat(Modifier.isAbstract(JobStatusSignal.class.getModifiers()), is(false));
    }

    @Test
    public void testJobStatusSuspendIsConcreteSubclass() {
        assertThat(JobStatus.class.isAssignableFrom(JobStatusSuspend.class), is(true));
        assertThat(Modifier.isAbstract(JobStatusSuspend.class.getModifiers()), is(false));
    }

    @Test
    public void testJobStatusDeclaresNoAdditionalMethods() {
        assertThat(JobStatus.class.getDeclaredMethods().length, is(0));
    }
}
