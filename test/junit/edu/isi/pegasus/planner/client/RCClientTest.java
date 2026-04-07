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
package edu.isi.pegasus.planner.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import gnu.getopt.LongOpt;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class RCClientTest {

    @Test
    public void testConstructorAndConstantDefaults() throws Exception {
        RCClient client = new RCClient("pegasus-rc-client");

        assertThat(getStaticField("LFN_DOES_NOT_EXIST_MSG"), is("LFN doesn't exist:"));
        assertThat(getStaticField("DEFAULT_CHUNK_FACTOR"), is(500));
        assertThat(ReflectionTestUtils.getField(client, "m_application"), is("pegasus-rc-client"));
    }

    @Test
    public void testEnterLowercasesKeysAndReturnsPreviousValue() throws Exception {
        RCClient client = new RCClient("pegasus-rc-client");
        setField(client, "m_prefs", new HashMap<String, String>());

        assertThat(client.enter("Format", "%l %p"), nullValue());
        assertThat(
                ((Map<?, ?>) ReflectionTestUtils.getField(client, "m_prefs")).get("format"),
                is("%l %p"));
        assertThat(client.enter("FORMAT", "%l %p %a"), is("%l %p"));
        assertThat(
                ((Map<?, ?>) ReflectionTestUtils.getField(client, "m_prefs")).get("format"),
                is("%l %p %a"));
    }

    @Test
    public void testGenerateValidOptions() {
        TestableRCClient client = new TestableRCClient("pegasus-rc-client");

        LongOpt[] options = client.exposedGenerateValidOptions();

        assertThat(options.length, is(11));
        assertThat(options[0].getName(), is("help"));
        assertThat(options[0].getHasArg(), is(LongOpt.NO_ARGUMENT));
        assertThat(options[2].getName(), is("file"));
        assertThat(options[2].getHasArg(), is(LongOpt.REQUIRED_ARGUMENT));
        assertThat(options[10].getName(), is("prefix"));
        assertThat(options[10].getHasArg(), is(LongOpt.REQUIRED_ARGUMENT));
    }

    @Test
    public void testLookupConfPropertyParsesLongAndShortOptionForms() throws Exception {
        RCClient client = new RCClient("pegasus-rc-client");
        Method method =
                RCClient.class.getDeclaredMethod("lookupConfProperty", String[].class, char.class);
        method.setAccessible(true);

        assertThat(
                method.invoke(
                        client,
                        new Object[] {
                            new String[] {"--conf", "/tmp/a.properties"}, Character.valueOf('c')
                        }),
                is("/tmp/a.properties"));
        assertThat(
                method.invoke(
                        client,
                        new Object[] {
                            new String[] {"-c", "/tmp/b.properties", "-P", ReplicaCatalog.c_prefix},
                            Character.valueOf('c')
                        }),
                is("/tmp/b.properties"));
        assertThat(
                method.invoke(
                        client,
                        new Object[] {
                            new String[] {"-P", ReplicaCatalog.c_prefix}, Character.valueOf('c')
                        }),
                nullValue());
    }

    private void setField(Object target, String name, Object value) throws Exception {
        Field field = RCClient.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object getStaticField(String name) throws Exception {
        return ReflectionTestUtils.getField(RCClient.class, name);
    }

    private static final class TestableRCClient extends RCClient {
        TestableRCClient(String appName) {
            super(appName);
        }

        LongOpt[] exposedGenerateValidOptions() {
            return generateValidOptions();
        }
    }
}
