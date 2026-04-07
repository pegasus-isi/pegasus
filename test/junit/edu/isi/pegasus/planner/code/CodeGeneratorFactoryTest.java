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
package edu.isi.pegasus.planner.code;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for CodeGeneratorFactory constants and structure */
public class CodeGeneratorFactoryTest {

    public static class TestCodeGenerator implements CodeGenerator {
        private PegasusBag mBag;

        @Override
        public void initialize(PegasusBag bag) {
            mBag = bag;
        }

        @Override
        public Collection<File> generateCode(ADag dag) {
            return Collections.emptyList();
        }

        @Override
        public void generateCode(ADag dag, Job job) {}

        @Override
        public boolean startMonitoring() {
            return false;
        }

        @Override
        public void reset() {}

        public PegasusBag getBag() {
            return mBag;
        }
    }

    @Test
    public void testDefaultPackageName() {
        assertThat(
                CodeGeneratorFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.code.generator"));
    }

    @Test
    public void testCondorCodeGeneratorClassName() {
        assertThat(
                CodeGeneratorFactory.CONDOR_CODE_GENERATOR_CLASS,
                is("edu.isi.pegasus.planner.code.generator.condor.CondorGenerator"));
    }

    @Test
    public void testStampedeEventGeneratorClassName() {
        assertThat(
                CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS,
                is("edu.isi.pegasus.planner.code.generator.Stampede"));
    }

    @Test
    public void testCondorGeneratorClassIsLoadable() throws ClassNotFoundException {
        // Verify the condor generator class actually exists on the classpath
        Class<?> clazz = Class.forName(CodeGeneratorFactory.CONDOR_CODE_GENERATOR_CLASS);
        assertThat(clazz, notNullValue());
    }

    @Test
    public void testStampedeGeneratorClassIsLoadable() throws ClassNotFoundException {
        Class<?> clazz = Class.forName(CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS);
        assertThat(clazz, notNullValue());
    }

    @Test
    public void testLoadInstanceThrowsWhenNullBag() {
        assertThrows(NullPointerException.class, () -> CodeGeneratorFactory.loadInstance(null));
    }

    @Test
    public void testLoadInstanceThrowsWhenBagHasNullProperties() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PLANNER_OPTIONS, new PlannerOptions());

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> CodeGeneratorFactory.loadInstance(bag));

        assertThat(exception.getMessage().contains("Invalid properties passed"), is(true));
    }

    @Test
    public void testLoadInstanceThrowsWhenBagHasNullOptions() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> CodeGeneratorFactory.loadInstance(bag));

        assertThat(exception.getMessage().contains("Invalid Options specified"), is(true));
    }

    @Test
    public void testLoadInstanceWithNullClassnameThrows() {
        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> CodeGeneratorFactory.loadInstance(configuredBag(), null));

        assertThat(exception.getMessage().contains("Invalid className specified"), is(true));
    }

    @Test
    public void testLoadInstanceWithUnknownClassWrapsFailure() {
        CodeGeneratorFactoryException exception =
                assertThrows(
                        CodeGeneratorFactoryException.class,
                        () -> CodeGeneratorFactory.loadInstance(configuredBag(), "does.not.Exist"));

        assertThat(exception.getMessage().contains("Instantiating Code Generator"), is(true));
    }

    @Test
    public void testLoadInstanceWithExplicitClassInitializesGenerator() throws Exception {
        PegasusBag bag = configuredBag();

        CodeGenerator generator =
                CodeGeneratorFactory.loadInstance(bag, TestCodeGenerator.class.getName());

        assertThat(generator, instanceOf(TestCodeGenerator.class));
        assertThat(((TestCodeGenerator) generator).getBag(), sameInstance(bag));
    }

    @Test
    public void testLoadInstanceUsesConfiguredGeneratorFromProperties() throws Exception {
        PegasusBag bag = configuredBag();
        bag.getPegasusProperties()
                .setProperty("pegasus.code.generator", TestCodeGenerator.class.getName());

        CodeGenerator generator = CodeGeneratorFactory.loadInstance(bag);

        assertThat(generator, instanceOf(TestCodeGenerator.class));
        assertThat(((TestCodeGenerator) generator).getBag(), sameInstance(bag));
    }

    private PegasusBag configuredBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, new PlannerOptions());
        return bag;
    }
}
