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
package edu.isi.pegasus.planner.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import gnu.getopt.LongOpt;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.griphyn.vdl.toolkit.Toolkit;
import org.junit.jupiter.api.Test;

/** Structural tests for the ExitCode client class via reflection. */
public class ExitCodeTest {

    @Test
    public void testExitCodeIsConcreteClass() {
        assertThat(Modifier.isAbstract(ExitCode.class.getModifiers()), is(false));
    }

    @Test
    public void testExitCodeExtendsToolkit() {
        assertThat(Toolkit.class.isAssignableFrom(ExitCode.class), is(true));
    }

    @Test
    public void testUsage1ConstantNotNull() {
        assertThat(ExitCode.m_usage1, notNullValue());
    }

    @Test
    public void testUsage1ConstantNotEmpty() {
        assertThat(ExitCode.m_usage1.isEmpty(), is(false));
    }

    @Test
    public void testExitCodeCanBeInstantiated() {
        ExitCode ec = new ExitCode("test-exitcode");
        assertThat(ec, notNullValue());
    }

    @Test
    public void testHasShowUsageMethod() throws NoSuchMethodException {
        Method showUsage = ExitCode.class.getMethod("showUsage");
        assertThat(showUsage, notNullValue());
    }

    @Test
    public void testHasGenerateValidOptionsMethod() throws NoSuchMethodException {
        // generateValidOptions() is protected, not public — use getDeclaredMethod
        Method genOpts = ExitCode.class.getDeclaredMethod("generateValidOptions");
        assertThat(genOpts, notNullValue());
    }

    @Test
    public void testGenerateValidOptionsReturns11Elements() {
        ExitCode ec = new ExitCode("test");
        gnu.getopt.LongOpt[] opts = ec.generateValidOptions();
        assertThat(opts.length, is(11));
    }

    @Test
    public void testUsage1ConstantHasExpectedValue() {
        assertThat(
                ExitCode.m_usage1,
                is("[-d dbprefix | -n | -N] [-e] [-f] [-i] [-v] [-l tag -m ISO] file [..]"));
    }

    @Test
    public void testGenerateValidOptionsContainsExpectedOptions() {
        ExitCode ec = new ExitCode("test");
        LongOpt[] opts = ec.generateValidOptions();

        assertThat(hasOption(opts, "help", 'h'), is(true));
        assertThat(hasOption(opts, "dbase", 'd'), is(true));
        assertThat(hasOption(opts, "version", 'V'), is(true));
        assertThat(hasOption(opts, "ignore", 'i'), is(true));
        assertThat(hasOption(opts, "noadd", 'n'), is(true));
        assertThat(hasOption(opts, "nofail", 'N'), is(true));
        assertThat(hasOption(opts, "emptyfail", 'e'), is(true));
        assertThat(hasOption(opts, "fail", 'f'), is(true));
        assertThat(hasOption(opts, "label", 'l'), is(true));
        assertThat(hasOption(opts, "mtime", 'm'), is(true));
    }

    @Test
    public void testShowUsagePrintsUsageAndExitCodes() {
        ExitCode ec = new ExitCode("test-exitcode");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            ec.showUsage();
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("Usage: test-exitcode"));
        assertThat(text, containsString("Generic options:"));
        assertThat(text, containsString("The following exit codes are returned"));
        assertThat(text, containsString("0  remote application ran to conclusion"));
    }

    private boolean hasOption(LongOpt[] options, String name, int value) {
        for (LongOpt option : options) {
            if (name.equals(option.getName()) && value == option.getVal()) {
                return true;
            }
        }
        return false;
    }
}
