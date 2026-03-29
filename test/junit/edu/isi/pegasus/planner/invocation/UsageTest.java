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

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Usage invocation class. */
public class UsageTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Usage.class));
    }

    @Test
    public void testDefaultConstructorZeroUserTime() {
        Usage u = new Usage();
        assertEquals(0.0, u.getUserTime(), 1e-9);
    }

    @Test
    public void testDefaultConstructorZeroSystemTime() {
        Usage u = new Usage();
        assertEquals(0.0, u.getSystemTime(), 1e-9);
    }

    @Test
    public void testSetAndGetUserTime() {
        Usage u = new Usage();
        u.setUserTime(1.234);
        assertEquals(1.234, u.getUserTime(), 1e-9);
    }

    @Test
    public void testSetAndGetSystemTime() {
        Usage u = new Usage();
        u.setSystemTime(0.567);
        assertEquals(0.567, u.getSystemTime(), 1e-9);
    }

    @Test
    public void testSetAndGetMinorFaults() {
        Usage u = new Usage();
        u.setMinorFaults(100);
        assertEquals(100, u.getMinorFaults());
    }

    @Test
    public void testSetAndGetMajorFaults() {
        Usage u = new Usage();
        u.setMajorFaults(5);
        assertEquals(5, u.getMajorFaults());
    }

    @Test
    public void testToXMLContainsUsageTag() throws Exception {
        Usage u = new Usage();
        u.setUserTime(2.5);
        StringWriter sw = new StringWriter();
        u.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("<usage"));
        assertTrue(xml.contains("utime="));
    }
}
