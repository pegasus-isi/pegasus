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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Version;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DagInfoTest {

    private DagInfo mDagInfo;

    @BeforeEach
    public void setUp() {
        mDagInfo = new DagInfo();
    }

    // --- Construction ---

    @Test
    public void defaultConstructorInitializesEmptyLabel() {
        // mNameOfADag is initialized to "" in constructor; getLabel returns it directly
        assertThat(mDagInfo.getLabel(), is(""));
    }

    @Test
    public void defaultConstructorInitializesEmptyIndex() {
        assertThat(mDagInfo.getIndex(), is(""));
    }

    @Test
    public void defaultConstructorInitializesEmptyCount() {
        assertThat(mDagInfo.getCount(), is(""));
    }

    @Test
    public void defaultConstructorInitializesEmptyDAXVersion() {
        assertThat(mDagInfo.getDAXVersion(), is(""));
    }

    @Test
    public void defaultConstructorHasNonNullWorkflowMetrics() {
        assertThat(mDagInfo.getWorkflowMetrics(), is(notNullValue()));
    }

    // --- setLabel / getLabel ---

    @Test
    public void setLabelUpdatesLabel() {
        mDagInfo.setLabel("myWorkflow");
        assertThat(mDagInfo.getLabel(), is("myWorkflow"));
    }

    @Test
    public void setLabelAlsoUpdatesWorkflowMetricsLabel() {
        mDagInfo.setLabel("metricWorkflow");

        assertThat(mDagInfo.getWorkflowMetrics().getLabel(), is("metricWorkflow"));
    }

    @Test
    public void setLabelToNullReturnsDefaultName() {
        mDagInfo.setLabel(null);
        assertThat(mDagInfo.getLabel(), is("PegasusRun"));
    }

    // --- setIndex / getIndex ---

    @Test
    public void setIndexUpdatesIndex() {
        mDagInfo.setIndex("3");
        assertThat(mDagInfo.getIndex(), is("3"));
    }

    // --- setCount / getCount ---

    @Test
    public void setCountUpdatesCount() {
        mDagInfo.setCount("5");
        assertThat(mDagInfo.getCount(), is("5"));
    }

    // --- setDAXVersion / getDAXVersion ---

    @Test
    public void setDAXVersionUpdatesVersion() {
        mDagInfo.setDAXVersion("3.0");
        assertThat(mDagInfo.getDAXVersion(), is("3.0"));
    }

    // --- setFlowTimestamp / getFlowTimestamp ---

    @Test
    public void setFlowTimestampUpdatesTimestamp() {
        mDagInfo.setFlowTimestamp("20230101T120000+0000");
        assertThat(mDagInfo.getFlowTimestamp(), is("20230101T120000+0000"));
    }

    // --- setDAXMTime(String) / getMTime ---

    @Test
    public void setDAXMTimeStringUpdatesTime() {
        mDagInfo.setDAXMTime("2023-01-01T00:00:00+0000");
        assertThat(mDagInfo.getMTime(), is("2023-01-01T00:00:00+0000"));
    }

    @Test
    public void setDAXMTimeFromFileUsesLastModifiedTime() throws IOException {
        File tempFile = Files.createTempFile("daginfo", ".dax").toFile();
        long timestamp = 1_700_000_000_000L;
        assertThat(tempFile.setLastModified(timestamp), is(true));

        mDagInfo.setDAXMTime(tempFile);

        assertThat(
                mDagInfo.getMTime(),
                is(Currently.iso8601(false, true, false, new java.util.Date(timestamp))));
    }

    // --- generateFlowName / getFlowName ---

    @Test
    public void generateFlowNameWithLabelAndIndex() {
        mDagInfo.setLabel("MyWF");
        mDagInfo.setIndex("0");
        mDagInfo.generateFlowName();
        assertThat(mDagInfo.getFlowName(), is("MyWF-0"));
    }

    @Test
    public void generateFlowNameWithNullLabelUsesDefault() {
        mDagInfo.setLabel(null);
        mDagInfo.setIndex("1");
        mDagInfo.generateFlowName();
        assertThat(mDagInfo.getFlowName(), is("PegasusRun-1"));
    }

    @Test
    public void getFlowNameReturnsEmptyStringBeforeGeneration() {
        // mFlowIDName is initialized to "" in constructor; auto-generation requires null
        assertThat(mDagInfo.getFlowName(), is(""));
    }

    // --- generateFlowID / getFlowID ---

    @Test
    public void generateFlowIDCombinesNameAndTimestamp() {
        mDagInfo.setLabel("TestWF");
        mDagInfo.setIndex("0");
        mDagInfo.generateFlowName();
        mDagInfo.setFlowTimestamp("20230101T000000+0000");
        mDagInfo.generateFlowID();
        assertThat(mDagInfo.getFlowID(), is("TestWF-0-20230101T000000+0000"));
    }

    @Test
    public void generateFlowIDResultContainsDash() {
        mDagInfo.setLabel("WF");
        mDagInfo.setIndex("1");
        mDagInfo.generateFlowName();
        mDagInfo.setFlowTimestamp("T1");
        mDagInfo.generateFlowID();
        assertThat(mDagInfo.getFlowID(), containsString("-"));
    }

    // --- updateLFNMap / getLFNs ---

    @Test
    public void updateLFNMapAndGetAllLFNs() {
        mDagInfo.updateLFNMap("job1", "input.txt", 'i');
        Set lfns = mDagInfo.getLFNs();
        assertThat(lfns.contains("input.txt"), is(true));
    }

    @Test
    public void getLFNsReturnsAllTypes() {
        mDagInfo.updateLFNMap("job1", "input.txt", 'i');
        mDagInfo.updateLFNMap("job2", "output.txt", 'o');
        Set lfns = mDagInfo.getLFNs();
        assertThat(lfns.size(), is(2));
    }

    @Test
    public void getLFNsInputOnlyIncludesInputFile() {
        mDagInfo.updateLFNMap("job1", "input.txt", 'i');
        mDagInfo.updateLFNMap("job2", "output.txt", 'o');
        Set lfns = mDagInfo.getLFNs(true);
        assertThat(lfns.contains("input.txt"), is(true));
    }

    @Test
    public void getLFNsInputOnlyExcludesOutputFile() {
        mDagInfo.updateLFNMap("job1", "input.txt", 'i');
        mDagInfo.updateLFNMap("job2", "output.txt", 'o');
        Set lfns = mDagInfo.getLFNs(true);
        assertThat(lfns.contains("output.txt"), is(false));
    }

    @Test
    public void getLFNsInputOnlyReturnsOnlyInputFiles() {
        mDagInfo.updateLFNMap("job1", "in1.txt", 'i');
        mDagInfo.updateLFNMap("job1", "in2.txt", 'i');
        Set lfns = mDagInfo.getLFNs(true);
        assertThat(lfns.size(), is(2));
    }

    @Test
    public void updateLFNMapUpgradesTypeToBothOnMismatch() {
        // First register as input, then as output → should become 'b' (both)
        mDagInfo.updateLFNMap("job1", "inter.txt", 'i');
        mDagInfo.updateLFNMap("job2", "inter.txt", 'o');
        // After upgrade, getLFNs(true) (input only) should NOT include it
        Set inputLfns = mDagInfo.getLFNs(true);
        assertThat(inputLfns.contains("inter.txt"), is(false));
        // But getLFNs() (all) should
        Set allLfns = mDagInfo.getLFNs(false);
        assertThat(allLfns.contains("inter.txt"), is(true));
    }

    @Test
    public void updateLFNMapDuplicateOutputThrowsException() {
        mDagInfo.updateLFNMap("job1", "out.txt", 'o');
        assertThrows(RuntimeException.class, () -> mDagInfo.updateLFNMap("job2", "out.txt", 'o'));
    }

    @Test
    public void updateLFNMapDuplicateInputKeepsSingleEntry() {
        mDagInfo.updateLFNMap("job1", "in.txt", 'i');
        mDagInfo.updateLFNMap("job2", "in.txt", 'i');

        assertThat(mDagInfo.getLFNs().size(), is(1));
        assertThat(mDagInfo.getLFNs(true).contains("in.txt"), is(true));
    }

    // --- computeDAXFileCounts ---

    @Test
    public void computeDAXFileCountsReturnsZeroForEmpty() {
        WorkflowMetrics metrics = mDagInfo.computeDAXFileCounts();
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input), is(0));
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output), is(0));
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate), is(0));
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total), is(0));
    }

    @Test
    public void computeDAXFileCountsCountsInputs() {
        mDagInfo.updateLFNMap("j1", "in1", 'i');
        mDagInfo.updateLFNMap("j1", "in2", 'i');
        WorkflowMetrics metrics = mDagInfo.computeDAXFileCounts();
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input), is(2));
    }

    @Test
    public void computeDAXFileCountsCountsOutputs() {
        mDagInfo.updateLFNMap("j1", "out1", 'o');
        WorkflowMetrics metrics = mDagInfo.computeDAXFileCounts();
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output), is(1));
    }

    @Test
    public void computeDAXFileCountsCountsIntermediate() {
        mDagInfo.updateLFNMap("j1", "inter", 'i');
        mDagInfo.updateLFNMap("j2", "inter", 'o'); // now type is 'b'
        WorkflowMetrics metrics = mDagInfo.computeDAXFileCounts();
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate), is(1));
    }

    @Test
    public void computeDAXFileCountsTotalIsSum() {
        mDagInfo.updateLFNMap("j1", "in", 'i');
        mDagInfo.updateLFNMap("j2", "out", 'o');
        WorkflowMetrics metrics = mDagInfo.computeDAXFileCounts();
        assertThat(metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total), is(2));
    }

    // --- clone ---

    @Test
    public void cloneCopiesLabel() {
        mDagInfo.setLabel("CloneTest");
        DagInfo cloned = (DagInfo) mDagInfo.clone();
        assertThat(cloned.getLabel(), is("CloneTest"));
    }

    @Test
    public void cloneCopiesIndex() {
        mDagInfo.setIndex("7");
        DagInfo cloned = (DagInfo) mDagInfo.clone();
        assertThat(cloned.getIndex(), is("7"));
    }

    @Test
    public void cloneCopiesCount() {
        mDagInfo.setCount("10");
        DagInfo cloned = (DagInfo) mDagInfo.clone();
        assertThat(cloned.getCount(), is("10"));
    }

    @Test
    public void cloneCopiesFlowTimestampAndReleaseVersion() {
        mDagInfo.setFlowTimestamp("20230101T120000+0000");
        mDagInfo.setReleaseVersion();

        DagInfo cloned = (DagInfo) mDagInfo.clone();

        assertThat(cloned.getFlowTimestamp(), is("20230101T120000+0000"));
        assertThat(cloned.getReleaseVersion(), is(mDagInfo.getReleaseVersion()));
    }

    @Test
    public void cloneCopiesLFNMap() {
        mDagInfo.updateLFNMap("j1", "file.txt", 'i');
        DagInfo cloned = (DagInfo) mDagInfo.clone();
        assertThat(cloned.getLFNs().contains("file.txt"), is(true));
    }

    @Test
    public void cloneIsIndependentFromOriginalLFNMap() {
        mDagInfo.updateLFNMap("j1", "file.txt", 'i');
        DagInfo cloned = (DagInfo) mDagInfo.clone();
        mDagInfo.updateLFNMap("j2", "extra.txt", 'o');
        assertThat(cloned.getLFNs().contains("extra.txt"), is(false));
    }

    // --- toString ---

    @Test
    public void toStringContainsLabel() {
        mDagInfo.setLabel("TestWorkflow");
        assertThat(mDagInfo.toString(), containsString("TestWorkflow"));
    }

    @Test
    public void toStringContainsIndex() {
        mDagInfo.setIndex("42");
        assertThat(mDagInfo.toString(), containsString("42"));
    }

    @Test
    public void toStringContainsDAXVersion() {
        mDagInfo.setDAXVersion("5.0");
        assertThat(mDagInfo.toString(), containsString("5.0"));
    }

    @Test
    public void setReleaseVersionUsesVersionUtility() {
        mDagInfo.setReleaseVersion();

        assertThat(mDagInfo.getReleaseVersion(), is(new Version().getVersion()));
    }
}
