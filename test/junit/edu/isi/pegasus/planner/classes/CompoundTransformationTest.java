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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.dax.Invoke;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CompoundTransformationTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    // -----------------------------------------------------------------------
    // construction
    // -----------------------------------------------------------------------

    @Test
    public void testConstructorWithNameOnly() {
        CompoundTransformation ct = new CompoundTransformation("myTransform");
        assertThat(ct.getName(), is("myTransform"));
        assertThat(ct.getNamespace(), is(""));
        assertThat(ct.getVersion(), is(""));
    }

    @Test
    public void testFullConstructor() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", "1.0");
        assertThat(ct.getNamespace(), is("ns"));
        assertThat(ct.getName(), is("name"));
        assertThat(ct.getVersion(), is("1.0"));
    }

    @Test
    public void testConstructorNullNamespaceBecomesEmpty() {
        CompoundTransformation ct = new CompoundTransformation(null, "name", "1.0");
        assertThat(ct.getNamespace(), is(""));
    }

    @Test
    public void testConstructorNullNameBecomesEmpty() {
        CompoundTransformation ct = new CompoundTransformation("ns", null, "1.0");
        assertThat(ct.getName(), is(""));
    }

    @Test
    public void testConstructorNullVersionBecomesEmpty() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", null);
        assertThat(ct.getVersion(), is(""));
    }

    @Test
    public void testDependentFilesInitiallyEmpty() {
        CompoundTransformation ct = new CompoundTransformation("t");
        assertThat(ct.getDependantFiles(), empty());
    }

    // -----------------------------------------------------------------------
    // dependent files
    // -----------------------------------------------------------------------

    @Test
    public void testAddAndGetDependantFile() {
        CompoundTransformation ct = new CompoundTransformation("t");
        PegasusFile pf = new PegasusFile("input.txt");
        ct.addDependantFile(pf);
        List<PegasusFile> files = ct.getDependantFiles();
        assertThat(files, hasSize(1));
        assertThat(files.get(0), is(pf));
    }

    @Test
    public void testAddMultipleDependantFiles() {
        CompoundTransformation ct = new CompoundTransformation("t");
        ct.addDependantFile(new PegasusFile("a.txt"));
        ct.addDependantFile(new PegasusFile("b.txt"));
        ct.addDependantFile(new PegasusFile("c.txt"));
        assertThat(ct.getDependantFiles(), hasSize(3));
    }

    // -----------------------------------------------------------------------
    // getCompleteName
    // -----------------------------------------------------------------------

    @Test
    public void testGetCompleteNameWithAllParts() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", "2.0");
        String name = ct.getCompleteName();
        // Separator.combine produces "ns::name:2.0" style
        assertThat(name, containsString("name"));
        assertThat(name, containsString("ns"));
        assertThat(name, containsString("2.0"));
    }

    @Test
    public void testGetCompleteNameWithNameOnly() {
        CompoundTransformation ct = new CompoundTransformation("myTx");
        String name = ct.getCompleteName();
        assertThat(name, containsString("myTx"));
    }

    // -----------------------------------------------------------------------
    // equals / hashCode
    // -----------------------------------------------------------------------

    @Test
    public void testEqualsSameObject() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", "1.0");
        assertEquals(ct, ct);
    }

    @Test
    public void testEqualsSymmetric() {
        CompoundTransformation ct1 = new CompoundTransformation("ns", "name", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns", "name", "1.0");
        assertEquals(ct1, ct2);
        assertEquals(ct2, ct1);
    }

    @Test
    public void testNotEqualsDifferentName() {
        CompoundTransformation ct1 = new CompoundTransformation("ns", "nameA", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns", "nameB", "1.0");
        assertNotEquals(ct1, ct2);
    }

    @Test
    public void testNotEqualsDifferentNamespace() {
        CompoundTransformation ct1 = new CompoundTransformation("ns1", "name", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns2", "name", "1.0");
        assertNotEquals(ct1, ct2);
    }

    @Test
    public void testNotEqualsDifferentVersion() {
        CompoundTransformation ct1 = new CompoundTransformation("ns", "name", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns", "name", "2.0");
        assertNotEquals(ct1, ct2);
    }

    @Test
    public void testNotEqualsNull() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", "1.0");
        assertNotEquals(ct, null);
    }

    @Test
    public void testNotEqualsDifferentClass() {
        CompoundTransformation ct = new CompoundTransformation("ns", "name", "1.0");
        assertNotEquals(ct, "some string");
    }

    @Test
    public void testHashCodeConsistentWithEquals() {
        CompoundTransformation ct1 = new CompoundTransformation("ns", "name", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns", "name", "1.0");
        assertEquals(ct1, ct2);
        assertThat(ct1.hashCode(), is(ct2.hashCode()));
    }

    @Test
    public void testHashCodeDifferentForDifferentObjects() {
        CompoundTransformation ct1 = new CompoundTransformation("ns", "alpha", "1.0");
        CompoundTransformation ct2 = new CompoundTransformation("ns", "beta", "1.0");
        // hash codes should differ (not guaranteed but almost certain for these inputs)
        assertThat(ct1.hashCode(), not(ct2.hashCode()));
    }

    // -----------------------------------------------------------------------
    // notifications
    // -----------------------------------------------------------------------

    @Test
    public void testNotificationsInitiallyEmpty() {
        CompoundTransformation ct = new CompoundTransformation("t");
        assertThat(ct.getNotifications(), notNullValue());
        // no notifications for any WHEN value
        assertThat(ct.getNotifications(Invoke.WHEN.start), empty());
        assertThat(ct.getNotifications(Invoke.WHEN.success), empty());
        assertThat(ct.getNotifications(Invoke.WHEN.end), empty());
    }

    @Test
    public void testAddAndGetNotification() {
        CompoundTransformation ct = new CompoundTransformation("t");
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        ct.addNotification(invoke);
        Collection<Invoke> notifications = ct.getNotifications(Invoke.WHEN.start);
        assertThat(notifications, hasSize(1));
    }

    @Test
    public void testAddNotificationsFromAnotherSet() {
        CompoundTransformation ct = new CompoundTransformation("t");
        Notifications ns = new Notifications();
        ns.add(new Invoke(Invoke.WHEN.end, "/bin/done.sh"));
        ns.add(new Invoke(Invoke.WHEN.end, "/bin/also.sh"));
        ct.addNotifications(ns);
        assertThat(ct.getNotifications(Invoke.WHEN.end), hasSize(2));
    }

    @Test
    public void testGetNotificationsReturnsAllWhenKinds() {
        CompoundTransformation ct = new CompoundTransformation("t");
        ct.addNotification(new Invoke(Invoke.WHEN.start, "/start.sh"));
        ct.addNotification(new Invoke(Invoke.WHEN.success, "/success.sh"));
        ct.addNotification(new Invoke(Invoke.WHEN.error, "/error.sh"));

        assertThat(ct.getNotifications(Invoke.WHEN.start), hasSize(1));
        assertThat(ct.getNotifications(Invoke.WHEN.success), hasSize(1));
        assertThat(ct.getNotifications(Invoke.WHEN.error), hasSize(1));
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsName() {
        CompoundTransformation ct = new CompoundTransformation("ns", "myTx", "1.0");
        String s = ct.toString();
        assertThat(s, containsString("myTx"));
        assertThat(s, containsString("Transformation"));
    }

    @Test
    public void testToStringContainsDependantFileInfo() {
        CompoundTransformation ct = new CompoundTransformation("myTx");
        PegasusFile pf = new PegasusFile("data.txt");
        // DATA_FILE type → label "data"
        pf.setType(PegasusFile.DATA_FILE);
        ct.addDependantFile(pf);
        String s = ct.toString();
        assertThat(s, containsString("data"));
    }

    @Test
    public void testToStringWithExecutableFile() {
        CompoundTransformation ct = new CompoundTransformation("myTx");
        PegasusFile pf = new PegasusFile("exe.sh");
        pf.setType(PegasusFile.EXECUTABLE_FILE);
        ct.addDependantFile(pf);
        String s = ct.toString();
        assertThat(s, containsString("executable"));
    }

    @Test
    public void testToStringContainsNotifications() {
        CompoundTransformation ct = new CompoundTransformation("myTx");
        ct.addNotification(new Invoke(Invoke.WHEN.start, "/notify.sh"));
        String s = ct.toString();
        assertThat(s, containsString("Notifications"));
    }
}
