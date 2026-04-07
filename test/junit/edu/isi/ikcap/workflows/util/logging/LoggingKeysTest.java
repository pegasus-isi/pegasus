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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class LoggingKeysTest {

    @Test
    public void testInterfaceShape() {
        assertThat(LoggingKeys.class.isInterface(), is(true));
        assertThat(LoggingKeys.class.getInterfaces().length, is(0));
    }

    @Test
    public void testRepresentativeConstantValues() {
        assertThat(LoggingKeys.MSG_ID, is("msgid"));
        assertThat(LoggingKeys.EVENT_ID_KEY, is("eventId"));
        assertThat(LoggingKeys.PROG, is("prog"));
        assertThat(LoggingKeys.CATALOG_URL, is("catalog.url"));
        assertThat(LoggingKeys.EVENT_PEGASUS_PLAN, is("event.pegasus.plan"));
        assertThat(LoggingKeys.DATA_CHARACTERIZATION_PROGRAM, is("DataCharacterization"));
        assertThat(LoggingKeys.PERFMETRIC_TIME_DURATION, is("perfmetric.time.duration"));
    }

    @Test
    public void testDeclaredFieldShape() throws Exception {
        Field msgId = LoggingKeys.class.getField("MSG_ID");
        Field dataCharacterization = LoggingKeys.class.getField("DATA_CHARACTERIZATION_PROGRAM");

        assertThat(msgId.getType(), is(String.class));
        assertThat(Modifier.isPublic(msgId.getModifiers()), is(true));
        assertThat(Modifier.isStatic(msgId.getModifiers()), is(true));
        assertThat(Modifier.isFinal(msgId.getModifiers()), is(true));

        assertThat(dataCharacterization.getType(), is(String.class));
        assertThat(Modifier.isPublic(dataCharacterization.getModifiers()), is(true));
        assertThat(Modifier.isStatic(dataCharacterization.getModifiers()), is(true));
        assertThat(Modifier.isFinal(dataCharacterization.getModifiers()), is(true));
    }

    @Test
    public void testDeclaredMethodAndFieldCounts() {
        assertThat(LoggingKeys.class.getDeclaredMethods().length, is(0));
        assertThat(LoggingKeys.class.getDeclaredFields().length, is(105));
    }
}
