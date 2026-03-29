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

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class StreamGobblerCallbackTest {

    @Test
    public void testLambdaImplementation_receivesLines() {
        List<String> received = new ArrayList<>();
        StreamGobblerCallback cb = line -> received.add(line);
        cb.work("line1");
        cb.work("line2");
        assertThat(received, hasItems("line1", "line2"));
        assertThat(received.size(), is(2));
    }

    @Test
    public void testAnonymousImplementation_work() {
        StringBuilder sb = new StringBuilder();
        StreamGobblerCallback cb =
                new StreamGobblerCallback() {
                    @Override
                    public void work(String line) {
                        sb.append(line);
                    }
                };
        cb.work("hello");
        assertThat(sb.toString(), is("hello"));
    }
}
