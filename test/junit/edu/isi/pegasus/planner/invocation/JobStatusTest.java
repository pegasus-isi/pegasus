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

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for JobStatus abstract class structure. */
public class JobStatusTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(JobStatus.class.getModifiers()));
    }

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(JobStatus.class));
    }

    @Test
    public void testJobStatusRegularIsConcreteSubclass() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusRegular.class));
        assertFalse(Modifier.isAbstract(JobStatusRegular.class.getModifiers()));
    }

    @Test
    public void testJobStatusFailureIsConcreteSubclass() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusFailure.class));
        assertFalse(Modifier.isAbstract(JobStatusFailure.class.getModifiers()));
    }

    @Test
    public void testJobStatusSignalIsConcreteSubclass() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusSignal.class));
        assertFalse(Modifier.isAbstract(JobStatusSignal.class.getModifiers()));
    }

    @Test
    public void testJobStatusSuspendIsConcreteSubclass() {
        assertTrue(JobStatus.class.isAssignableFrom(JobStatusSuspend.class));
        assertFalse(Modifier.isAbstract(JobStatusSuspend.class.getModifiers()));
    }
}
