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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Rafael Ferreira da Silva */
public class JDBCRCTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private JDBCRC jdbcrc = null;

    public JDBCRCTest() {}

    @Before
    public void setUp() throws IOException {

        String basename = "pegasus-db-admin";
        File pegasusDBAdmin = FindExecutable.findExec(basename);
        if (pegasusDBAdmin == null) {
            throw new RuntimeException("Unable to find path to " + basename);
        }
        String command = pegasusDBAdmin.getAbsolutePath() + " create jdbc:sqlite:jdbcrc_test.db";

        try {
            mTestSetup = new DefaultTestSetup();
            mLogger =
                    mTestSetup.loadLogger(
                            mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
            mLogger.logEventStart("test.pegasus.url", "setup", "0");

            Runtime r = Runtime.getRuntime();
            String[] envp = {"PYTHONPATH=" + System.getProperty("externals.python.path")};
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
            if (status != 0) {
                throw new RuntimeException(
                        "Database creation failed with non zero exit status " + command);
            }

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
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }

    @Test
    public void multipleSimpleInsert() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "handle"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", "handle")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c", "handle")));
    }

    @Test
    public void insertMultipleResourceHandles() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "x"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "y"));

        assertEquals("b", jdbcrc.lookup("a", "x"));
        assertEquals("b", jdbcrc.lookup("a", "y"));
    }

    @Test
    public void deleteByLFNandPFN() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "x"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "y"));
        jdbcrc.delete("a", "b");

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertFalse(c.contains(new ReplicaCatalogEntry("b")));
        assertFalse(c.contains(new ReplicaCatalogEntry("b", "x")));
        assertFalse(c.contains(new ReplicaCatalogEntry("b", "y")));
    }

    @Test
    public void deleteSpecificMapping() {
        HashMap attr =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
                        put("name", "value");
                    }
                };
        HashMap attr2 =
                new HashMap() {
                    {
                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "y");
                        put("name", "value");
                    }
                };

        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));
        jdbcrc.insert(
                "a",
                new ReplicaCatalogEntry(
                        "b",
                        new HashMap() {
                            {
                                put(ReplicaCatalogEntry.RESOURCE_HANDLE, "y");
                            }
                        }));
        jdbcrc.delete(
                "a",
                new ReplicaCatalogEntry(
                        "b",
                        new HashMap() {
                            {
                                put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
                            }
                        }));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr)));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr2)));

        jdbcrc.delete("a", new ReplicaCatalogEntry("b", attr));
        c = jdbcrc.lookup("a");
        assertFalse(c.contains(new ReplicaCatalogEntry("b", attr)));
        assertFalse(
                c.contains(
                        new ReplicaCatalogEntry(
                                "b",
                                new HashMap() {
                                    {
                                        put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
                                    }
                                })));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr2)));

        jdbcrc.delete("a", new ReplicaCatalogEntry("b", attr2));
        c = jdbcrc.lookup("a");
        assertFalse(c.contains(new ReplicaCatalogEntry("b", attr2)));
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

        assertFalse(c.contains(new ReplicaCatalogEntry("d", attr)));
        assertTrue(c.contains(new ReplicaCatalogEntry("d", attr2)));
        assertFalse(c.contains(new ReplicaCatalogEntry("d", attr3)));
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
        assertFalse(c.contains(new ReplicaCatalogEntry("b", attr)));

        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));
        c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr)));
    }

    @Test
    public void lookupCount() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "c"));
        jdbcrc.insert("a", new ReplicaCatalogEntry("d", "e"));
        jdbcrc.insert("f", new ReplicaCatalogEntry("g", "c"));

        HashMap attr = new HashMap();
        Map map = jdbcrc.lookup(attr);
        assertEquals(2, map.size());

        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "c");
        map = jdbcrc.lookup(attr);
        assertEquals(2, map.size());

        attr.put("pfn", "b");
        map = jdbcrc.lookup(attr);
        assertEquals(1, map.size());
    }

    @After
    public void tearDown() {
        jdbcrc.delete("a", "b");
        jdbcrc.delete("a", "c");
        jdbcrc.delete("a", "d");
        jdbcrc.close();
        new File("jdbcrc_test.db").delete();
    }
}
