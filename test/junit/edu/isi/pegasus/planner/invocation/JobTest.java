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

import java.util.Date;
import org.junit.jupiter.api.Test;

/** Tests for Job invocation class. */
public class JobTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Job.class));
    }

    @Test
    public void testConstructorWithTag() {
        Job j = new Job("mainjob");
        assertEquals("mainjob", j.getTag());
    }

    @Test
    public void testSetAndGetTag() {
        Job j = new Job("mainjob");
        j.setTag("postjob");
        assertEquals("postjob", j.getTag());
    }

    @Test
    public void testSetAndGetPID() {
        Job j = new Job("mainjob");
        j.setPID(12345);
        assertEquals(12345, j.getPID());
    }

    @Test
    public void testSetAndGetDuration() {
        Job j = new Job("mainjob");
        j.setDuration(3.14);
        assertEquals(3.14, j.getDuration(), 0.001);
    }

    @Test
    public void testSetAndGetStart() {
        Job j = new Job("mainjob");
        Date now = new Date();
        j.setStart(now);
        assertEquals(now, j.getStart());
    }

    @Test
    public void testNullUsageByDefault() {
        Job j = new Job("mainjob");
        assertNull(j.getUsage());
    }

    @Test
    public void testSetAndGetUsage() {
        Job j = new Job("mainjob");
        Usage u = new Usage();
        j.setUsage(u);
        assertNotNull(j.getUsage());
    }
}
