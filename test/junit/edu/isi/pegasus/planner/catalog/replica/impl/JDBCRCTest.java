/*
 * Copyright 2007-2014 University Of Southern California
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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class JDBCRCTest {

    private JDBCRC jdbcrc = null;

    public JDBCRCTest() {
    }

    @Before
    public void setUp() throws IOException {

        try {
            jdbcrc = new JDBCRC(
                    "org.sqlite.JDBC",
                    "jdbc:sqlite:jdbcrc_test.db",
                    "root", ""
            );

            String command = "./bin/pegasus-db-admin create jdbc:sqlite:jdbcrc_test.db";
            Runtime r = Runtime.getRuntime();
            Process p = r.exec(command);
            int status = p.waitFor();
            if (status != 0) {
                throw new RuntimeException("Database creation failed with non zero exit status " + command);
            }
        } catch (InterruptedException ie) {
            //ignore
        } catch (ClassNotFoundException ex) {
            throw new IOException(ex);
        } catch (SQLException ex) {
            throw new IOException(ex);
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
        HashMap attr = new HashMap();
        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
        attr.put("name", "value");
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));

        HashMap attr2 = new HashMap();
        attr2.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "x");
        jdbcrc.delete("a", new ReplicaCatalogEntry("b", attr2));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr)));

        jdbcrc.delete("a", new ReplicaCatalogEntry("b", attr));
        c = jdbcrc.lookup("a");
        assertFalse(c.contains(new ReplicaCatalogEntry("b", attr)));
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
        jdbcrc.insert("a", new ReplicaCatalogEntry("d", attr2));

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("d", attr)));
        assertTrue(c.contains(new ReplicaCatalogEntry("d", attr2)));
    }

    @Test
    public void updateToAttributesMap() {
        jdbcrc.insert("a", new ReplicaCatalogEntry("b", "z"));

        HashMap attr = new HashMap();
        attr.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "z");
        attr.put("key", "value");

        Collection<ReplicaCatalogEntry> c = jdbcrc.lookup("a");
        assertFalse(c.contains(new ReplicaCatalogEntry("b", attr)));

        jdbcrc.insert("a", new ReplicaCatalogEntry("b", attr));
        c = jdbcrc.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b", attr)));
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
