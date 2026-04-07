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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for InvocationRecord class. */
public class InvocationRecordTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(InvocationRecord.class), is(true));
    }

    @Test
    public void testImplementsSerializable() {
        assertThat(Serializable.class.isAssignableFrom(InvocationRecord.class), is(true));
    }

    @Test
    public void testSchemaNamespace() {
        assertThat(
                InvocationRecord.SCHEMA_NAMESPACE, is("https://pegasus.isi.edu/schema/invocation"));
    }

    @Test
    public void testSchemaLocation() {
        assertThat(InvocationRecord.SCHEMA_LOCATION.endsWith(".xsd"), is(true));
    }

    @Test
    public void testDefaultConstructorCreates() {
        InvocationRecord ir = new InvocationRecord();
        assertThat(ir, is(notNullValue()));
    }

    @Test
    public void testSetAndGetTransformation() {
        InvocationRecord ir = new InvocationRecord();
        ir.setTransformation("pegasus::findrange");
        assertThat(ir.getTransformation(), is("pegasus::findrange"));
    }

    @Test
    public void testSetAndGetUID() {
        InvocationRecord ir = new InvocationRecord();
        ir.setUID(1001);
        assertThat(ir.getUID(), is(1001));
    }

    @Test
    public void testSetAndGetUser() {
        InvocationRecord ir = new InvocationRecord();
        ir.setUser("testuser");
        assertThat(ir.getUser(), is("testuser"));
    }

    @Test
    public void testDefaultConstructorInitializesEmptyJobAndStatCollections() {
        InvocationRecord ir = new InvocationRecord();

        assertThat(ir.getJobCount(), is(0));
        assertThat(ir.getStatCount(), is(0));
        assertThat(ir.getJobList().isEmpty(), is(true));
        assertThat(ir.getStatList().isEmpty(), is(true));
    }

    @Test
    public void testAddSetAndRemoveJobLifecycle() {
        InvocationRecord ir = new InvocationRecord();
        Job first = new Job("main");
        Job second = new Job("post");
        Job replacement = new Job("replacement");

        ir.addJob(first);
        ir.addJob(0, second);
        assertThat(ir.getJobCount(), is(2));
        assertThat(ir.getJob(0), is(sameInstance(second)));
        assertThat(ir.getJob(1), is(sameInstance(first)));

        ir.setJob(1, replacement);
        assertThat(ir.getJob(1), is(sameInstance(replacement)));
        assertThat(ir.removeJob(0), is(sameInstance(second)));
        assertThat(ir.getJobCount(), is(1));
    }

    @Test
    public void testSetJobCollectionReplacesExistingJobsAndExposesUnmodifiableList() {
        InvocationRecord ir = new InvocationRecord();
        ir.addJob(new Job("old"));
        List<Job> jobs = Arrays.asList(new Job("one"), new Job("two"));

        ir.setJob(jobs);

        assertThat(ir.getJobCount(), is(2));
        assertThrows(
                UnsupportedOperationException.class, () -> ir.getJobList().add(new Job("three")));
    }

    @Test
    public void testAddSetAndRemoveStatCallLifecycle() {
        InvocationRecord ir = new InvocationRecord();
        StatCall first = new StatCall();
        StatCall second = new StatCall();
        StatCall replacement = new StatCall();

        ir.addStatCall(first);
        ir.addStatCall(0, second);
        assertThat(ir.getStatCount(), is(2));
        assertThat(ir.getStatCall(0), is(sameInstance(second)));
        assertThat(ir.getStatCall(1), is(sameInstance(first)));

        ir.setStatCall(1, replacement);
        assertThat(ir.getStatCall(1), is(sameInstance(replacement)));
        assertThat(ir.removeStatCall(0), is(sameInstance(second)));
        assertThat(ir.getStatCount(), is(1));
    }

    @Test
    public void testEnvironmentWorkingDirectoryAndWorkflowFieldsRoundTrip() {
        InvocationRecord ir = new InvocationRecord();
        Environment environment = new Environment();
        Date stamp = new Date(123456789L);

        environment.addEntry("PATH", "/usr/bin");
        ir.setEnvironment(environment);
        ir.setWorkingDirectory("/tmp/work");
        ir.setWorkflowLabel("label");
        ir.setWorkflowTimestamp(stamp);
        ir.setResource("condorpool");
        ir.setUMask(18);

        assertThat(ir.getEnvironment(), is(sameInstance(environment)));
        assertThat(ir.getWorkingDirectory().getValue(), is("/tmp/work"));
        assertThat(ir.getWorkflowLabel(), is("label"));
        assertThat(ir.getWorkflowTimestamp(), is(sameInstance(stamp)));
        assertThat(ir.getResource(), is("condorpool"));
        assertThat(ir.getUMask(), is(18));
    }
}
