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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** Tests for Job invocation class. */
public class JobTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Job.class), is(true));
    }

    @Test
    public void testConstructorWithTag() {
        Job j = new Job("mainjob");
        assertThat(j.getTag(), is("mainjob"));
    }

    @Test
    public void testSetAndGetTag() {
        Job j = new Job("mainjob");
        j.setTag("postjob");
        assertThat(j.getTag(), is("postjob"));
    }

    @Test
    public void testSetAndGetPID() {
        Job j = new Job("mainjob");
        j.setPID(12345);
        assertThat(j.getPID(), is(12345));
    }

    @Test
    public void testSetAndGetDuration() {
        Job j = new Job("mainjob");
        j.setDuration(3.14);
        assertThat(j.getDuration(), is(org.hamcrest.Matchers.closeTo(3.14, 0.001)));
    }

    @Test
    public void testSetAndGetStart() {
        Job j = new Job("mainjob");
        Date now = new Date();
        j.setStart(now);
        assertThat(j.getStart(), is(sameInstance(now)));
    }

    @Test
    public void testNullUsageByDefault() {
        Job j = new Job("mainjob");
        assertThat(j.getUsage(), is(nullValue()));
    }

    @Test
    public void testSetAndGetUsage() {
        Job j = new Job("mainjob");
        Usage u = new Usage();
        j.setUsage(u);
        assertThat(j.getUsage(), is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testDefaultStatusExecutableAndArgumentsAreNull() {
        Job j = new Job("mainjob");

        assertThat(j.getStatus(), is(nullValue()));
        assertThat(j.getExecutable(), is(nullValue()));
        assertThat(j.getArguments(), is(nullValue()));
    }

    @Test
    public void testSetAndGetStatus() {
        Job j = new Job("mainjob");
        Status status = new Status();

        j.setStatus(status);

        assertThat(j.getStatus(), is(sameInstance(status)));
    }

    @Test
    public void testSetAndGetExecutable() {
        Job j = new Job("mainjob");
        StatCall executable = new StatCall("exe");

        j.setExecutable(executable);

        assertThat(j.getExecutable(), is(sameInstance(executable)));
    }

    @Test
    public void testSetAndGetArguments() {
        Job j = new Job("mainjob");
        Arguments arguments = new ArgString("/bin/echo", "hello");

        j.setArguments(arguments);

        assertThat(j.getArguments(), is(sameInstance(arguments)));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Job j = new Job("mainjob");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> j.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }
}
