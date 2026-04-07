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
package edu.isi.ikcap.workflows.util.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.*;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class EventLogMessageTest {

    @Test
    public void testConstructorAndToStringPopulateEventAndTimestamp() {
        EventLogMessage message = new EventLogMessage("event.sample");

        String text = message.toString();

        assertThat(text, containsString("event=event.sample "));
        assertThat(
                text, matchesPattern(".*ts=\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z.*"));
        assertThat(message.toString(), is(text));
    }

    @Test
    public void testAddMethodsChainPrimitiveAndPairValues() {
        EventLogMessage message =
                new EventLogMessage("event.sample")
                        .setTimeStampMillis(1234L)
                        .add("text", "value")
                        .add("int", 7)
                        .add("long", 9L)
                        .add("float", 1.5f)
                        .add("double", 2.75d)
                        .addPair("pair", "left", "right")
                        .addTime("time", 0L);

        String text = message.toString();

        assertThat(text, containsString("ts=" + expectedRenderedTimestamp(1234L) + " "));
        assertThat(text, containsString("text=value "));
        assertThat(text, containsString("int=7 "));
        assertThat(text, containsString("long=9 "));
        assertThat(text, containsString("float=1.5 "));
        assertThat(text, containsString("double=2.75 "));
        assertThat(text, containsString("pair=(left,right) "));
        assertThat(text, containsString("time=" + expectedRenderedTime(0L) + " "));
    }

    @Test
    public void testAddWQAndAddMsgEscapeQuotes() {
        EventLogMessage message =
                new EventLogMessage("event.sample")
                        .setTimeStampMillis(0)
                        .addWQ("quoted", "a\"b")
                        .addMsg("hello \"world\"");

        String text = message.toString();

        assertThat(text, containsString("quoted=\"a\\\"b\" "));
        assertThat(text, containsString("msg=\"hello \\\"world\\\"\" "));
    }

    @Test
    public void testAddMapAndListUseCurrentFormatting() {
        Map<String, String> map = new LinkedHashMap<String, String>();
        map.put("alpha", "one");
        map.put("beta", null);

        EventLogMessage message =
                new EventLogMessage("event.sample")
                        .setTimeStampMillis(1234L)
                        .addMap("map", map)
                        .addList("list", Arrays.asList("x", "y"));

        String text = message.toString();

        assertThat(text, containsString("map=\"(alpha,one)(beta)\" "));
        assertThat(text, containsString("list=\"x,y\" "));
    }

    @Test
    public void testSetTimeStampMillisOverridesTimestampDeterministically() {
        EventLogMessage message = new EventLogMessage("event.sample").setTimeStampMillis(1234L);

        assertThat(
                message.toString(), containsString("ts=" + expectedRenderedTimestamp(1234L) + " "));
    }

    private String expectedRenderedTimestamp(long millis) {
        return expectedRenderedTime(millis) + String.format(".%06dZ", (millis % 1000L) * 1000L);
    }

    private String expectedRenderedTime(long millis) {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date(millis));
    }
}
