/*
 * Copyright 2007-2020 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.replica.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rafael Ferreira da Silva */
public class JDBCRCTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private JDBCRC jdbcrc = null;

    public JDBCRCTest() {}

    @BeforeEach
    public void setUp() throws IOException {

        String basename = "pegasus-db-admin";
        File pegasusDBAdmin = FindExecutable.findExec(basename);
        assumeTrue(pegasusDBAdmin != null, "Unable to find path to " + basename);
        String command = pegasusDBAdmin.getAbsolutePath() + " create jdbc:sqlite:jdbcrc_test.db";

        try {
            mTestSetup = new DefaultTestSetup();
            mLogger =
                    mTestSetup.loadLogger(
                            mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
            mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
            mLogger.logEventStart("test.pegasus.url", "setup", "0");

            Runtime r = Runtime.getRuntime();
            String[] envp = {"PYTHONPATH=" + System.getProperty("externals.python.path")};
            mLogger.log("Executing command " + command, LogManager.INFO_MESSAGE_LEVEL);
            Process p = r.exec(command, envp);

            // spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                    new StreamGobbler(
                            p.getInputStream(),
                            new DefaultStreamGobblerCallback(LogManager.CONSOLE_MESSAGE_LEVEL));
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new DefaultStreamGobblerCallback(LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            int status = p.waitFor();
            assumeTrue(
                    status == 0,
                    "Skipping because pegasus-db-admin is unavailable in this environment: "
                            + command);

            Properties props = new Properties();
            props.setProperty("db.driver", "sqlite");
            props.setProperty("db.url", "jdbc:sqlite:jdbcrc_test.db");

            jdbcrc = new JDBCRC();
            jdbcrc.connect(props);

        } catch (IOException ioe) {
            mLogger.log(
                    "IOException while executing " + command, ioe, LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException("IOException while executing " + command, ioe);
        } catch (InterruptedException ie) {
        }
    }

    @Test
    public void simpleInsert() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
    }

    @Test
    public void multipleSimpleInsert() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "handle"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "handle")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", "handle")), is(true));
    }

    @Test
    public void insertMultipleResourceHandles() {
        HashMap attr = new HashMap();
        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
        attr.put("bk", "bvalue");
        attr.put("bk2", "bvalue2");
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));

        HashMap attr2 = new HashMap();
        attr2.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "y");
        attr2.put("bk", "bvalue");
        attr2.put("bk2", "bvalue2");
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr2));

        assertThat(jdbcrc.lookup("a", "x"), is("b"));
        assertThat(jdbcrc.lookup("a", "y"), is("b"));
    }

    @Test
    public void deleteByLFNandPFN() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "x"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "y"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c", "y"));
        jdbcrc.delete("a", "b");

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "x")), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "y")), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("c", "y")), is(true));
    }

    @Test
    public void deleteSpecificMapping() {
        HashMap attr =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "e");
                        put("k", "v");
                        put("u", "v");
                        put("y", "x");
                    }
                };
        HashMap attr2 =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "f");
                        put("k", "v");
                        put("u", "v");
                        put("y", "x");
                    }
                };
        HashMap attr3 =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "d");
                        put("k", "v");
                        put("u", "v");
                        put("y", "x");
                    }
                };
        HashMap attr4 =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "g");
                        put("k", "v");
                        put("u", "v");
                        put("y", "x");
                    }
                };
        HashMap attr5 =
                new HashMap() {
                    {
                        put("h", "p");
                        put("i", "p");
                        put("k", "v");
                        put("u", "s");
                        put("y", "x");
                    }
                };
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr2));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c", attr3));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c", attr4));
        jdbcrc.insert("w", new ReplicaCatalogEntry("t", attr5));
        jdbcrc.insert("w", new ReplicaCatalogEntry("z", attr5));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr2)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr3)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr4)), is(true));

        jdbcrc.delete(
                "a",
                new ReplicaCatalogEntry(
                        "b",
                        new HashMap() {
                            {
                                put(ReplicaCatalogEntry.RESOURCE_HANDLE, "e");
                            }
                        }));
        c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr)), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr2)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr3)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr4)), is(true));

        jdbcrc.delete(
                "a",
                new ReplicaCatalogEntry(
                        "b",
                        new HashMap() {
                            {
                                put(ReplicaCatalogEntry.RESOURCE_HANDLE, "f");
                                put("u", "v");
                            }
                        }));
        c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr2)), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr3)), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("c", attr4)), is(false));
        HashMap attr6 =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "f");
                        put("k", "v");
                        put("y", "x");
                    }
                };
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr6)), is(true));
    }

    @Test
    public void simpleUpdate() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("d"));

        HashMap attr = new HashMap();
        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
        jdbcrc.insert("a", new ReplicaCatalogEntry("d", attr));

        HashMap attr2 = new HashMap();
        attr2.put("key", "value");
        attr2.put("key2", "value2");
        jdbcrc.insert("a", new ReplicaCatalogEntry("d", attr2));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        HashMap attr3 = new HashMap();
        attr3.put("key", "value");

        assertThat(c.contains(new ReplicaCatalogEntry("d", attr)), is(false));
        assertThat(c.contains(new ReplicaCatalogEntry("d", attr2)), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("d", attr3)), is(false));
    }

    @Test
    public void updateToAttributesMap() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "z"));

        HashMap attr =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "z");
                        put("key", "value");
                    }
                };

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr)), is(false));

        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));
        c = jdbcrc.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b", attr)), is(true));
    }

    @Test
    public void lookupCount() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "c"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("d", "e"));
        jdbcrc.insert("f", new ReplicaCatalogEntry("g", "c"));

        HashMap attr = new HashMap();
        Map map = jdbcrc.lookup(attr);
        assertThat(map.size(), is(2));

        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "c");
        map = jdbcrc.lookup(attr);
        assertThat(map.size(), is(2));

        attr.put("pfn", "b");
        map = jdbcrc.lookup(attr);
        assertThat(map.size(), is(1));
    }

    @AfterEach
    public void tearDown() {
        if (jdbcrc != null) {
            jdbcrc.close();
        }
        new File("jdbcrc_test.db").delete();
    }
}
