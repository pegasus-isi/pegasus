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
package edu.isi.pegasus.planner.code.generator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Braindump code generator class constants and structure. */
public class BraindumpTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
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
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    private static final class TestBraindump extends Braindump {
        void setSubmitDir(File dir) {
            this.mSubmitFileDir = dir.getAbsolutePath();
        }

        File write(Map<String, String> entries) throws Exception {
            return writeOutBraindumpFile(entries);
        }
    }

    @Test
    public void testBraindumpFileConstant() {
        assertThat(Braindump.BRAINDUMP_FILE, is("braindump.yml"));
    }

    @Test
    public void testGeneratorTypeKeyConstant() {
        assertThat(Braindump.GENERATOR_TYPE_KEY, is("type"));
    }

    @Test
    public void testUserKeyConstant() {
        assertThat(Braindump.USER_KEY, is("user"));
    }

    @Test
    public void testUUIDKeyConstant() {
        assertThat(Braindump.UUID_KEY, is("wf_uuid"));
    }

    @Test
    public void testRootUUIDKeyConstant() {
        assertThat(Braindump.ROOT_UUID_KEY, is("root_wf_uuid"));
    }

    @Test
    public void testDAXLabelKeyConstant() {
        assertThat(Braindump.DAX_LABEL_KEY, is("dax_label"));
    }

    @Test
    public void testSubmitDirKeyConstant() {
        assertThat(Braindump.SUBMIT_DIR_KEY, is("submit_dir"));
    }

    @Test
    public void testPlannerVersionKeyConstant() {
        assertThat(Braindump.PLANNER_VERSION_KEY, is("planner_version"));
    }

    @Test
    public void testTimestampKeyConstant() {
        assertThat(Braindump.TIMESTAMP_KEY, is("timestamp"));
    }

    @Test
    public void testPropertiesKeyConstant() {
        assertThat(Braindump.PROPERTIES_KEY, is("properties"));
    }

    @Test
    public void testPlannerUsedPMCReadsFlagFromBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);

        assertThat(Braindump.plannerUsedPMC(bag), is(true));
    }

    @Test
    public void testPlannerUsedPMCInfersMPIExecFromProperties() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty("pegasus.clusterer.job.aggregator", "MPIExec");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        assertThat(Braindump.plannerUsedPMC(bag), is(true));
    }

    @Test
    public void testLoadFromMissingBraindumpReturnsEmptyMap() throws Exception {
        File dir = Files.createTempDirectory("braindump-missing").toFile();

        assertThat(Braindump.loadFrom(dir).isEmpty(), is(true));
    }

    @Test
    public void testWriteOutBraindumpFileRoundTripsThroughLoadFrom() throws Exception {
        File dir = Files.createTempDirectory("braindump-roundtrip").toFile();
        TestBraindump braindump = new TestBraindump();
        braindump.setSubmitDir(dir);
        Map<String, String> entries = new LinkedHashMap<String, String>();
        entries.put(Braindump.USER_KEY, "alice");
        entries.put(Braindump.UUID_KEY, "uuid-1");

        File file = braindump.write(entries);
        Map<String, String> loaded = Braindump.loadFrom(dir);

        assertThat(file, is(new File(dir, Braindump.BRAINDUMP_FILE)));
        assertThat(loaded.get(Braindump.USER_KEY), is("alice"));
        assertThat(loaded.get(Braindump.UUID_KEY), is("uuid-1"));
    }

    @Test
    public void testGenerateCodeForSingleJobThrows() {
        Braindump braindump = new Braindump();
        CodeGeneratorException exception =
                assertThrows(
                        CodeGeneratorException.class,
                        () -> braindump.generateCode(new ADag(), new Job()));

        assertThat(exception.getMessage(), containsString("whole workflow"));
    }
}
