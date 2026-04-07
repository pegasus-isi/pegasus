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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Usage invocation class. */
public class UsageTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Usage.class), is(true));
    }

    @Test
    public void testDefaultConstructorZeroUserTime() {
        Usage u = new Usage();
        assertThat(u.getUserTime(), closeTo(0.0, 1e-9));
    }

    @Test
    public void testDefaultConstructorZeroSystemTime() {
        Usage u = new Usage();
        assertThat(u.getSystemTime(), closeTo(0.0, 1e-9));
    }

    @Test
    public void testSetAndGetUserTime() {
        Usage u = new Usage();
        u.setUserTime(1.234);
        assertThat(u.getUserTime(), closeTo(1.234, 1e-9));
    }

    @Test
    public void testSetAndGetSystemTime() {
        Usage u = new Usage();
        u.setSystemTime(0.567);
        assertThat(u.getSystemTime(), closeTo(0.567, 1e-9));
    }

    @Test
    public void testSetAndGetMinorFaults() {
        Usage u = new Usage();
        u.setMinorFaults(100);
        assertThat(u.getMinorFaults(), is(100));
    }

    @Test
    public void testSetAndGetMajorFaults() {
        Usage u = new Usage();
        u.setMajorFaults(5);
        assertThat(u.getMajorFaults(), is(5));
    }

    @Test
    public void testToXMLContainsUsageTag() throws Exception {
        Usage u = new Usage();
        u.setUserTime(2.5);
        StringWriter sw = new StringWriter();
        u.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("<usage"));
        assertThat(xml, containsString("utime="));
    }

    @Test
    public void testFullConstructorPopulatesRepresentativeFields() {
        Usage u = new Usage(1.25, 2.5, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

        assertThat(u.getUserTime(), closeTo(1.25, 1e-9));
        assertThat(u.getSystemTime(), closeTo(2.5, 1e-9));
        assertThat(u.getMinorFaults(), is(3));
        assertThat(u.getMajorFaults(), is(4));
        assertThat(u.getSwaps(), is(5));
        assertThat(u.getSignals(), is(6));
        assertThat(u.getVoluntarySwitches(), is(7));
        assertThat(u.getInvoluntarySwitches(), is(8));
        assertThat(u.getMaximumRSS(), is(9));
        assertThat(u.getSharedRSS(), is(10));
        assertThat(u.getUnsharedRSS(), is(11));
        assertThat(u.getStackRSS(), is(12));
        assertThat(u.getInputBlocks(), is(13));
        assertThat(u.getOutputBlocks(), is(14));
        assertThat(u.getSent(), is(15));
        assertThat(u.getReceived(), is(16));
    }

    @Test
    public void testSetAndGetRemainingCounters() {
        Usage u = new Usage();
        u.setSwaps(5);
        u.setSignals(6);
        u.setVoluntarySwitches(7);
        u.setInvoluntarySwitches(8);
        u.setMaximumRSS(9);
        u.setSharedRSS(10);
        u.setUnsharedRSS(11);
        u.setStackRSS(12);
        u.setInputBlocks(13);
        u.setOutputBlocks(14);
        u.setSent(15);
        u.setReceived(16);

        assertThat(u.getSwaps(), is(5));
        assertThat(u.getSignals(), is(6));
        assertThat(u.getVoluntarySwitches(), is(7));
        assertThat(u.getInvoluntarySwitches(), is(8));
        assertThat(u.getMaximumRSS(), is(9));
        assertThat(u.getSharedRSS(), is(10));
        assertThat(u.getUnsharedRSS(), is(11));
        assertThat(u.getStackRSS(), is(12));
        assertThat(u.getInputBlocks(), is(13));
        assertThat(u.getOutputBlocks(), is(14));
        assertThat(u.getSent(), is(15));
        assertThat(u.getReceived(), is(16));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Usage u = new Usage();

        IOException exception =
                assertThrows(IOException.class, () -> u.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vdl-support@griphyn.org"));
    }

    @Test
    public void testToXMLUsesNamespaceAndFormatsTimesToThreeDecimals() throws Exception {
        Usage u = new Usage();
        u.setUserTime(1.2);
        u.setSystemTime(0.5678);
        u.setSwaps(5);
        u.setSignals(6);

        StringWriter sw = new StringWriter();
        u.toXML(sw, "  ", "inv");

        String xml = sw.toString();
        assertThat(xml.startsWith("  <inv:usage"), is(true));
        assertThat(xml, containsString("utime=\"1.200\""));
        assertThat(xml, containsString("stime=\"0.568\""));
        assertThat(xml, containsString("nswap=\"5\""));
        assertThat(xml, containsString("nsignals=\"6\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }
}
