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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import gnu.getopt.LongOpt;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the CPlanner client class via reflection.
 *
 * @author Rajiv Mayani
 */
public class CPlannerTest {

    @Test
    public void testCPlannerIsConcreteClass() {
        assertThat(Modifier.isAbstract(CPlanner.class.getModifiers()), is(false));
    }

    @Test
    public void testCPlannerExtendsExecutable() {
        Class<?> superClass = CPlanner.class.getSuperclass();
        assertThat(superClass, notNullValue());
        assertThat(superClass, is(Executable.class));
    }

    @Test
    public void testCPlannerHasMainMethod() throws NoSuchMethodException {
        Method main = CPlanner.class.getMethod("main", String[].class);
        assertThat(main, notNullValue());
        assertThat(Modifier.isStatic(main.getModifiers()), is(true));
        assertThat(Modifier.isPublic(main.getModifiers()), is(true));
    }

    @Test
    public void testCPlannerHasGenerateValidOptionsMethod() throws NoSuchMethodException {
        Method genOpts = CPlanner.class.getMethod("generateValidOptions");
        assertThat(genOpts, notNullValue());
    }

    @Test
    public void testCPlannerCanBeInstantiatedWithNoArgConstructor() {
        // CPlanner(LogManager) constructor - pass null for no-dependency instantiation
        CPlanner planner = new CPlanner();
        assertThat(planner, notNullValue());
    }

    @Test
    public void testCPlannerGeneratesOptions() {
        CPlanner planner = new CPlanner();
        gnu.getopt.LongOpt[] opts = planner.generateValidOptions();
        assertThat(opts, notNullValue());
        assertThat(opts.length, greaterThan(0));
    }

    @Test
    public void testCPlannerConstantsHaveExpectedValues() {
        assertThat(CPlanner.CLEANUP_DIR, is("cleanup"));
        assertThat(CPlanner.NOOP_PREFIX, is("noop_"));
        assertThat(CPlanner.PEGASUS_MONITORD_LAUNCH_PROPERTY_KEY, is("pegasus.monitord"));
        assertThat(CPlanner.DEFAULT_WORKFLOW_DAX_FILE, is("workflow.yml"));
        assertThat(
                CPlanner.JAVA_COMMAND_LINE_PROPERTY_REGEX,
                is("(env|condor|globus|dagman|pegasus)\\..*=.*"));
    }

    @Test
    public void testGenerateValidOptionsIncludesExpectedLongOptions() {
        CPlanner planner = new CPlanner();
        LongOpt[] opts = planner.generateValidOptions();

        assertThat(opts.length, is(33));
        assertThat(hasOption(opts, "dir", '8'), is(true));
        assertThat(hasOption(opts, "dax", 'd'), is(true));
        assertThat(hasOption(opts, "conf", '6'), is(true));
        assertThat(hasOption(opts, "cleanup", '1'), is(true));
        assertThat(hasOption(opts, "json", 'J'), is(true));
        assertThat(hasOption(opts, "transformations-dir", 't'), is(true));
    }

    @Test
    public void testPrintShortVersionWritesUsageText() {
        CPlanner planner = new CPlanner();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            planner.printShortVersion();
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("Usage : pegasus-plan"));
        assertThat(text, containsString("--cleanup"));
        assertThat(text, containsString("abstract-workflow"));
    }

    @Test
    public void testPrintLongVersionWritesOptionsSection() {
        CPlanner planner = new CPlanner();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            planner.printLongVersion();
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("pegasus-plan - The main command line client"));
        assertThat(text, containsString("Options"));
        assertThat(text, containsString("--staging-site"));
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
