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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract container shell wrapper class. */
public class AbstractTest {

    private static class TestContainerShellWrapper extends Abstract {

        @Override
        public String wrap(Job job) {
            return "job";
        }

        @Override
        public String wrap(AggregatedJob job) {
            return "aggregated";
        }

        @Override
        public String describe() {
            return "test";
        }

        String renderMessage(String prefix, String message) {
            StringBuilder sb = new StringBuilder();
            appendStderrFragment(sb, prefix, message);
            return sb.toString();
        }

        String renderTransferInput(Collection<FileTransfer> files, PegasusFile.LINKAGE linkage) {
            return convertToTransferInputFormat(files, linkage).toString();
        }
    }

    @Test
    public void testAbstractImplementsContainerShellWrapper() {
        assertThat(ContainerShellWrapper.class.isAssignableFrom(Abstract.class), is(true));
    }

    @Test
    public void testSeparatorConstant() {
        assertThat(Abstract.SEPARATOR, is("########################"));
    }

    @Test
    public void testSeparatorCharConstant() {
        assertThat(Abstract.SEPARATOR_CHAR, is('#'));
    }

    @Test
    public void testPegasusLiteMessagePrefix() {
        assertThat(Abstract.PEGASUS_LITE_MESSAGE_PREFIX, is("[Pegasus Lite]"));
    }

    @Test
    public void testContainerMessagePrefix() {
        assertThat(Abstract.CONTAINER_MESSAGE_PREFIX, is("[Container]"));
    }

    @Test
    public void testMessageStringLength() {
        assertThat(Abstract.MESSAGE_STRING_LENGTH, is(80));
    }

    @Test
    public void testNoneExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(None.class), is(true));
    }

    @Test
    public void testAbstractClassIsAbstract() {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
    }

    @Test
    public void testAppendStderrFragmentFormatsMessage() {
        TestContainerShellWrapper wrapper = new TestContainerShellWrapper();

        String result = wrapper.renderMessage(Abstract.CONTAINER_MESSAGE_PREFIX, "starting");

        assertThat(result, startsWith("printf \"\\n"));
        assertThat(result, containsString("[Container] starting"));
        assertThat(result, endsWith("\\n\"  1>&2\n"));
    }

    @Test
    public void testAppendStderrFragmentRejectsOverlongMessage() {
        TestContainerShellWrapper wrapper = new TestContainerShellWrapper();
        String message =
                "x"
                        .repeat(
                                Abstract.MESSAGE_STRING_LENGTH
                                        - Abstract.CONTAINER_MESSAGE_PREFIX.length()
                                        + 1);

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> wrapper.renderMessage(Abstract.CONTAINER_MESSAGE_PREFIX, message));

        assertThat(e.getMessage(), containsString("exceedss 80 characters"));
    }

    @Test
    public void testComplainForHeadNodeFileServerIncludesJobName() {
        TestContainerShellWrapper wrapper = new TestContainerShellWrapper();

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> wrapper.complainForHeadNodeFileServer("jobA", "condorpool"));

        assertThat(e.getMessage(), containsString("For job (jobA)."));
        assertThat(e.getMessage(), containsString("site: condorpool"));
    }

    @Test
    public void testConvertToTransferInputFormatIncludesExpectedFields() {
        TestContainerShellWrapper wrapper = new TestContainerShellWrapper();
        FileTransfer transfer = new FileTransfer("f.in", "jobA");
        transfer.addSource("local", "file:///data/f.in");
        transfer.addDestination("condorpool", "file:///scratch/f.in");
        transfer.setVerifySymlinkSource(false);

        String result =
                wrapper.renderTransferInput(
                        Collections.singletonList(transfer), PegasusFile.LINKAGE.input);

        assertThat(result, containsString("\"type\": \"transfer\""));
        assertThat(result, containsString("\"linkage\": \"input\""));
        assertThat(result, containsString("\"lfn\": \"f.in\""));
        assertThat(result, containsString("\"site_label\": \"local\""));
        assertThat(result, containsString("\"url\": \"file:///data/f.in\""));
        assertThat(result, containsString("\"site_label\": \"condorpool\""));
        assertThat(result, containsString("\"verify_symlink_source\": false"));
    }
}
