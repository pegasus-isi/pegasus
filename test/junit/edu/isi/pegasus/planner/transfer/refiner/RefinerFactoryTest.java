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
package edu.isi.pegasus.planner.transfer.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.implementation.TransferImplementationFactoryException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class RefinerFactoryTest {

    @Test
    public void testConstants() {
        assertThat(
                RefinerFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.transfer.refiner"));
        assertThat(RefinerFactory.DEFAULT_REFINER_IMPLEMENTATION, is("BalancedCluster"));
    }

    @Test
    public void testLoadInstanceWithShortClassNameUsesDefaultPackageName() {
        PegasusBag bag = createMinimalBag();
        ADag dag = new ADag();
        Refiner refiner = RefinerFactory.loadInstance(null, bag, dag);
        assertThat(refiner, notNullValue());
        assertThat(refiner.getClass(), sameInstance(BalancedCluster.class));
    }

    @Test
    public void testLoadInstanceWithExplicitClassNameLoadsRefinerAndInvokesLoadImplementations() {
        TestRefiner.reset();

        ADag dag = new ADag();
        PegasusBag bag = createMinimalBag();

        Refiner refiner = RefinerFactory.loadInstance(TestRefiner.class.getName(), bag, dag);

        assertThat(refiner, notNullValue());
        assertThat(refiner.getClass(), sameInstance(TestRefiner.class));
        assertThat(TestRefiner.loadImplementationsCalled, is(true));
        assertThat(TestRefiner.lastBagPassedToLoadImplementations, sameInstance(bag));
    }

    @Test
    public void testLoadInstanceUsesPropertyDrivenClassName() {
        TestRefiner.reset();

        ADag dag = new ADag();
        PegasusBag bag = createMinimalBag();
        bag.getPegasusProperties()
                .setProperty("pegasus.transfer.refiner", TestRefiner.class.getName());

        Refiner refiner = RefinerFactory.loadInstance(dag, bag);

        assertThat(refiner, notNullValue());
        assertThat(refiner.getClass(), sameInstance(TestRefiner.class));
        assertThat(TestRefiner.loadImplementationsCalled, is(true));
    }

    @Test
    public void testLoadInstanceRejectsNullWorkflow() {
        PegasusBag bag = createMinimalBag();

        TransferRefinerFactoryException exception =
                assertThrows(
                        TransferRefinerFactoryException.class,
                        () -> RefinerFactory.loadInstance("TestRefiner", bag, null));

        assertThat(exception.getMessage(), is("Instantiating Transfer Refiner"));
        assertThat(exception.getClassname(), is("TestRefiner"));
        assertThat(exception.getCause(), notNullValue());
        assertThat(exception.getCause().getMessage(), is("Invalid workflow passed"));
    }

    @Test
    public void testLoadInstanceWithNullClassNameDefaultsToBalancedCluster() {
        PegasusBag bag = createMinimalBag();
        ADag dag = new ADag();
        Refiner refiner = RefinerFactory.loadInstance(null, bag, dag);
        assertThat(refiner, notNullValue());
        assertThat(refiner.getClass(), sameInstance(BalancedCluster.class));
    }

    private PegasusBag createMinimalBag() {
        PegasusBag bag = new PegasusBag();

        PlannerOptions options = new PlannerOptions();
        options.setExecutionSites(Collections.singleton("local"));

        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        return bag;
    }

    public static class TestRefiner extends Basic {

        static boolean loadImplementationsCalled;
        static PegasusBag lastBagPassedToLoadImplementations;

        public TestRefiner(ADag dag, PegasusBag bag) {
            super(dag, bag);
        }

        static void reset() {
            loadImplementationsCalled = false;
            lastBagPassedToLoadImplementations = null;
        }

        @Override
        public void loadImplementations(PegasusBag bag)
                throws TransferImplementationFactoryException {
            loadImplementationsCalled = true;
            lastBagPassedToLoadImplementations = bag;
        }
    }
}

class NoOpLogManager extends LogManager {

    @Override
    public void initialize(LogFormatter formatter, Properties properties) {
        this.mLogFormatter = formatter;
    }

    @Override
    public void configure(boolean prefixTimestamp) {}

    @Override
    protected void setLevel(int level, boolean info) {}

    @Override
    public int getLevel() {
        return LogManager.DEBUG_MESSAGE_LEVEL;
    }

    @Override
    public void setWriters(String out) {}

    @Override
    public void setWriter(STREAM_TYPE type, PrintStream ps) {}

    @Override
    public PrintStream getWriter(STREAM_TYPE type) {
        return System.out;
    }

    @Override
    public void log(String message, Exception e, int level) {}

    @Override
    public void log(String message, int level) {}

    @Override
    protected void logAlreadyFormattedMessage(String message, int level) {}

    @Override
    public void logEventCompletion(int level) {}
}
