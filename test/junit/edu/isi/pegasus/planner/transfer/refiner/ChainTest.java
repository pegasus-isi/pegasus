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
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ChainTest {

    @Test
    public void testChainExtendsBasicAndConstants() {
        assertThat(Chain.class.getSuperclass(), sameInstance(Basic.class));
        assertThat(Chain.DEFAULT_BUNDLE_FACTOR, is("1"));
        assertThat(
                Chain.DESCRIPTION,
                is("Chain Mode (the stage in jobs being chained together in bundles"));
    }

    @Test
    public void testConstructorInitializesMapsAndSiteStoreField() throws Exception {
        Chain chain = createChain();

        Map<?, ?> stageInMap = (Map<?, ?>) ReflectionTestUtils.getField(chain, "mStageInMap");
        assertThat(stageInMap instanceof Map, is(true));
        assertThat(stageInMap.isEmpty(), is(true));

        Map<?, ?> siBundleMap = (Map<?, ?>) ReflectionTestUtils.getField(chain, "mSIBundleMap");
        assertThat(siBundleMap instanceof Map, is(true));
        assertThat(siBundleMap.isEmpty(), is(true));

        assertThat(ReflectionTestUtils.getField(chain, "mSiteStore"), nullValue());
    }

    @Test
    public void testAddRelationWithParentNewFalseCurrentlyOverflowsStack() {
        Chain chain = createChain();

        assertThrows(
                StackOverflowError.class, () -> chain.addRelation("tx1", "job1", "siteA", false));
    }

    @Test
    public void testAddRelationWithParentNewTrueCurrentlyOverflowsStack() throws Exception {
        Chain chain = createChain();

        @SuppressWarnings("unchecked")
        Map<String, Integer> bundleMap =
                (Map<String, Integer>) ReflectionTestUtils.getField(chain, "mSIBundleMap");
        bundleMap.put("siteA", 1);

        assertThrows(
                StackOverflowError.class, () -> chain.addRelation("tx1", "jobA", "siteA", true));
    }

    @Test
    public void testMethodAndInnerClassStructure() throws Exception {
        Method descriptionMethod = Chain.class.getDeclaredMethod("getDescription");
        assertThat(descriptionMethod.getReturnType(), sameInstance(String.class));
        assertThat(Modifier.isPublic(descriptionMethod.getModifiers()), is(true));

        Method siteBundleMethod =
                Chain.class.getDeclaredMethod("getSiteBundleValue", String.class, String.class);
        assertThat(siteBundleMethod.getReturnType(), sameInstance(int.class));
        assertThat(Modifier.isPublic(siteBundleMethod.getModifiers()), is(true));

        Class<?>[] innerClasses = Chain.class.getDeclaredClasses();
        boolean hasSiteTransfer = false;
        boolean hasTransferChain = false;
        for (Class<?> innerClass : innerClasses) {
            if (innerClass.getSimpleName().equals("SiteTransfer")) {
                hasSiteTransfer = true;
                assertThat(Modifier.isPrivate(innerClass.getModifiers()), is(true));
                assertThat(Modifier.isStatic(innerClass.getModifiers()), is(true));
            }
            if (innerClass.getSimpleName().equals("TransferChain")) {
                hasTransferChain = true;
                assertThat(Modifier.isPrivate(innerClass.getModifiers()), is(true));
                assertThat(Modifier.isStatic(innerClass.getModifiers()), is(true));
            }
        }

        assertThat(hasSiteTransfer, is(true));
        assertThat(hasTransferChain, is(true));
    }

    private Chain createChain() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setExecutionSites(Collections.singleton("local"));

        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        return new Chain(new ADag(), bag);
    }

    private static class NoOpLogManager extends LogManager {

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
}
