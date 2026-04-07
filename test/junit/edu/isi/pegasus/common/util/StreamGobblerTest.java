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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class StreamGobblerTest {

    @Test
    public void testStreamGobblerExtendsThread() {
        assertThat(Thread.class.isAssignableFrom(StreamGobbler.class), is(true));
    }

    @Test
    public void testConstructor_doesNotThrow() {
        ByteArrayInputStream in = new ByteArrayInputStream("line\n".getBytes());
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(() -> new StreamGobbler(in, cb));
    }

    @Test
    public void testRun_callsCallbackForEachLine() throws InterruptedException {
        String data = "line1\nline2\nline3\n";
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());

        List<String> captured = new ArrayList<>();
        StreamGobblerCallback cb = line -> captured.add(line);

        StreamGobbler gobbler = new StreamGobbler(in, cb);
        gobbler.start();
        gobbler.join(5000);

        assertThat(captured, hasItems("line1", "line2", "line3"));
    }

    @Test
    public void testRun_emptyStream_noCallbacks() throws InterruptedException {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        List<String> captured = new ArrayList<>();
        StreamGobblerCallback cb = line -> captured.add(line);

        StreamGobbler gobbler = new StreamGobbler(in, cb);
        gobbler.start();
        gobbler.join(5000);

        assertThat(captured, is(empty()));
    }

    @Test
    public void testRedirect_beforeStart_doesNotThrow() {
        ByteArrayInputStream in = new ByteArrayInputStream("data\n".getBytes());
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        StreamGobbler gobbler = new StreamGobbler(in, cb);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertDoesNotThrow(() -> gobbler.redirect(out, ">> "));
    }

    @Test
    public void testClose_canBeCalledSafely() {
        ByteArrayInputStream in = new ByteArrayInputStream("data\n".getBytes());
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        StreamGobbler gobbler = new StreamGobbler(in, cb);
        assertDoesNotThrow(() -> gobbler.close());
    }

    @Test
    public void testClose_canBeCalledTwiceSafely() {
        ByteArrayInputStream in = new ByteArrayInputStream("data\n".getBytes());
        StreamGobbler gobbler = new StreamGobbler(in, line -> {});
        gobbler.close();
        assertDoesNotThrow(() -> gobbler.close());
    }

    @Test
    public void testRedirect_storesOutputStreamAndPrompt() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream("line1\nline2\n".getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamGobbler gobbler = new StreamGobbler(in, line -> {});
        gobbler.redirect(out, ">> ");

        assertThat(ReflectionTestUtils.getField(gobbler, "mOPStream"), is(out));
        assertThat(ReflectionTestUtils.getField(gobbler, "mPrompt"), is(">> "));
    }

    @Test
    public void testRedirect_nullPromptDefaultsToEmptyPrefix() throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream("onlyline\n".getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StreamGobbler gobbler = new StreamGobbler(in, line -> {});
        gobbler.redirect(out, null);

        assertThat(ReflectionTestUtils.getField(gobbler, "mPrompt"), is(""));
    }
}
