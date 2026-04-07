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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.xml.sax.helpers.AttributesImpl;

/** @author Rajiv Mayani */
public class DAXParser2Test {

    @Test
    public void testConstructorCapturesJobPrefixAndStartsWithDefaultFlags() throws Exception {
        PlannerOptions options = new PlannerOptions();
        options.setJobnamePrefix("pref-");
        PegasusBag bag = createBag(options);

        DAXParser2 parser = new DAXParser2(bag, "3.0");

        assertThat(getField(parser, "mJobPrefix"), is("pref-"));
        assertThat(getBooleanField(parser, "mUseDoubleNegative"), is(false));
        assertThat(parser.getDAXCallback(), is(nullValue()));
    }

    @Test
    public void testSetAndGetDAXCallback() {
        DAXParser2 parser = new DAXParser2(createBag(null), "3.0");
        NoOpCallback callback = new NoOpCallback();

        parser.setDAXCallback(callback);

        assertThat(parser.getDAXCallback(), is(sameInstance(callback)));
    }

    @Test
    public void testSchemaVersionHelpersAndDoubleNegativeSelection() {
        TestableDAXParser2 parser = new TestableDAXParser2(createBag(null), "3.0");

        assertThat(
                parser.extractVersionFromSchema(
                        "https://pegasus.isi.edu/schema/DAX https://pegasus.isi.edu/schema/dax-2.0.xsd"),
                is("2.0"));
        assertThat(
                parser.extractVersionFromSchema(
                        "https://pegasus.isi.edu/schema/DAX no-version-here"),
                is(nullValue()));
        assertThat(parser.exposedUseDoubleNegative("2.0"), is(true));
        assertThat(parser.exposedUseDoubleNegative("2.1"), is(false));
        assertThat(parser.exposedUseDoubleNegative("3.0"), is(false));
        assertThat((double) parser.shiftRight("2.1"), closeTo(21.0, 0.0));
    }

    @Test
    public void testGetSchemaOfDocumentAndVersionOfDAX() throws IOException {
        DAXParser2 parser = new DAXParser2(createBag(null), "3.0");
        Path dax = Files.createTempFile("dax-parser2-", ".dax");
        Files.write(
                dax,
                ("<?xml version=\"1.0\"?>\n"
                                + "<adag xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                                + "xsi:schemaLocation=\"https://pegasus.isi.edu/schema/DAX https://pegasus.isi.edu/schema/dax-2.2.xsd\" "
                                + "name=\"wf\"/>")
                        .getBytes());

        assertThat(
                parser.getSchemaOfDocument(dax.toString()),
                is(
                        "https://pegasus.isi.edu/schema/DAX https://pegasus.isi.edu/schema/dax-2.2.xsd"));
        assertThat(parser.getVersionOfDAX(dax.toString()), is("2.2"));
    }

    @Test
    public void testGetSchemaLocationAndAdagStartCallback() throws Exception {
        TestableDAXParser2 parser = new TestableDAXParser2(createBag(null), "3.0");
        CapturingCallback callback = new CapturingCallback();
        parser.setDAXCallback(callback);

        setField(parser, "mDaxSchemaVersion", "2.0");
        String olderSchema = parser.getSchemaLocation();
        assertThat(olderSchema.endsWith("dax-2.0.xsd"), is(true));

        setField(parser, "mDaxSchemaVersion", "9.9");
        String defaultSchema = parser.getSchemaLocation();
        assertThat(defaultSchema.endsWith("dax-3.0.xsd"), is(true));

        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", "name", "name", "CDATA", "wf.name+1");
        attrs.addAttribute("", "count", "count", "CDATA", "1");
        parser.startElement("", "adag", "adag", attrs);

        assertThat(callback.documentAttributes, is(notNullValue()));
        assertThat(callback.documentAttributes.get("name"), is("wf_name_1"));
        assertThat(callback.documentAttributes.get("count"), is("1"));
    }

    private PegasusBag createBag(PlannerOptions options) {
        PegasusBag bag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        if (options != null) {
            bag.add(PegasusBag.PLANNER_OPTIONS, options);
        }
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private void setField(Object target, String name, Object value) throws Exception {
        ReflectionTestUtils.setField(target, name, value);
    }

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
    }

    private static final class TestableDAXParser2 extends DAXParser2 {
        TestableDAXParser2(PegasusBag bag, String schemaVersion) {
            super(bag, schemaVersion);
        }

        boolean exposedUseDoubleNegative(String daxVersion) {
            return useDoubleNegative(daxVersion);
        }
    }

    private static class NoOpCallback implements Callback {
        @Override
        public void initialize(PegasusBag bag, String dax) {}

        @Override
        public Object getConstructedObject() {
            return null;
        }

        @Override
        public void cbDocument(Map attributes) {}

        @Override
        public void cbWfInvoke(Invoke invoke) {}

        @Override
        public void cbMetadata(Profile p) {}

        @Override
        public void cbJob(Job job) {}

        @Override
        public void cbParents(String child, java.util.List<PCRelation> parents) {}

        @Override
        public void cbChildren(String parent, java.util.List<String> children) {}

        @Override
        public void cbDone() {}

        @Override
        public void cbCompoundTransformation(CompoundTransformation compoundTransformation) {}

        @Override
        public void cbFile(ReplicaLocation rl) {}

        @Override
        public void cbExecutable(TransformationCatalogEntry tce) {}

        @Override
        public void cbReplicaStore(ReplicaStore store) {}

        @Override
        public void cbTransformationStore(TransformationStore store) {}

        @Override
        public void cbSiteStore(SiteStore store) {}
    }

    private static final class CapturingCallback extends NoOpCallback {
        private Map documentAttributes;

        @Override
        public void cbDocument(Map attributes) {
            this.documentAttributes = attributes;
        }
    }

    private static final class NoOpLogManager extends LogManager {

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
            return null;
        }

        @Override
        public void log(String message, int level) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void logEventStart(String name, String entityName, String entityID) {}

        @Override
        public void logEventStart(String name, String entityName, String entityID, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
