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
package org.griphyn.vdl.toolkit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.LongOpt;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TestPropsTest {

    /*
    @Test
    public void testSomeMethod() {
        assertEquals(1, 1);
    }
    */

    @Test
    public void generateValidOptionsMatchesCliContract() {
        TestProps props = new TestProps("show-properties");

        LongOpt[] options = props.generateValidOptions();

        assertThat(options.length, is(5));
        assertThat(options[0].getName(), is("version"));
        assertThat(options[0].getVal(), is((int) 'V'));
        assertThat(options[1].getName(), is("help"));
        assertThat(options[1].getVal(), is((int) 'h'));
        assertThat(options[2].getName(), is("verbose"));
        assertThat(options[2].getVal(), is(1));
        assertThat(options[3].getName(), is("unsorted"));
        assertThat(options[3].getVal(), is((int) 'u'));
        assertThat(options[4].getName(), is("concise"));
        assertThat(options[4].getVal(), is((int) 'c'));
    }

    @Test
    public void showUsagePrintsExpectedFlags() {
        TestProps props = new TestProps("show-properties");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8.name()));

            props.showUsage();
        } catch (Exception e) {
            fail(e);
        } finally {
            System.setOut(original);
        }

        String usage = new String(output.toByteArray(), StandardCharsets.UTF_8);
        assertThat(usage, containsString("Usage: show-properties [-c] [-u] | [-V]"));
        assertThat(usage, containsString("-c|--concise"));
        assertThat(usage, containsString("-u|--unsorted"));
        assertThat(usage, containsString("-V|--version"));
    }

    @Test
    public void mainWithVersionFlagPrintsVersionAndReturns() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8.name()));

            TestProps.main(new String[] {"-V"});
        } catch (Exception e) {
            fail(e);
        } finally {
            System.setOut(original);
        }

        String text = new String(output.toByteArray(), StandardCharsets.UTF_8);
        assertThat(text, containsString("$Id$"));
        assertThat(text, containsString("VDS version " + Version.instance().toString()));
    }
}
