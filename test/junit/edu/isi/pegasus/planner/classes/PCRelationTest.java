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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PCRelationTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorInitializesEmptyStrings() {
        PCRelation rel = new PCRelation();
        assertThat(rel.getParent(), is(""));
        assertThat(rel.getChild(), is(""));
    }

    @Test
    public void testDefaultConstructorIsDeletedFalse() {
        PCRelation rel = new PCRelation();
        assertFalse(rel.isDeleted);
    }

    @Test
    public void testTwoArgConstructorSetsParentAndChild() {
        PCRelation rel = new PCRelation("parentJob", "childJob");
        assertThat(rel.getParent(), is("parentJob"));
        assertThat(rel.getChild(), is("childJob"));
    }

    @Test
    public void testTwoArgConstructorIsDeletedDefaultsFalse() {
        PCRelation rel = new PCRelation("p", "c");
        assertFalse(rel.isDeleted);
    }

    @Test
    public void testThreeArgConstructorSetsDeletedFlag() {
        PCRelation rel = new PCRelation("parentJob", "childJob", true);
        assertTrue(rel.isDeleted);
    }

    @Test
    public void testSetAndGetAbstractParentID() {
        PCRelation rel = new PCRelation("p", "c");
        rel.setAbstractParentID("abstractP");
        assertThat(rel.getAbstractParentID(), is("abstractP"));
    }

    @Test
    public void testSetAndGetAbstractChildID() {
        PCRelation rel = new PCRelation("p", "c");
        rel.setAbstractChildID("abstractC");
        assertThat(rel.getAbstractChildID(), is("abstractC"));
    }

    @Test
    public void testDefaultConstructorAbstractIDsAreEmpty() {
        PCRelation rel = new PCRelation();
        assertThat(rel.getAbstractParentID(), is(""));
        assertThat(rel.getAbstractChildID(), is(""));
    }

    @Test
    public void testEqualsSameParentAndChild() {
        PCRelation rel1 = new PCRelation("p", "c");
        PCRelation rel2 = new PCRelation("p", "c");
        assertEquals(rel1, rel2);
    }

    @Test
    public void testEqualsDifferentParentNotEqual() {
        PCRelation rel1 = new PCRelation("p1", "c");
        PCRelation rel2 = new PCRelation("p2", "c");
        assertNotEquals(rel1, rel2);
    }

    @Test
    public void testEqualsDifferentChildNotEqual() {
        PCRelation rel1 = new PCRelation("p", "c1");
        PCRelation rel2 = new PCRelation("p", "c2");
        assertNotEquals(rel1, rel2);
    }

    @Test
    public void testEqualityIgnoresDeletedFlag() {
        PCRelation rel1 = new PCRelation("p", "c", true);
        PCRelation rel2 = new PCRelation("p", "c", false);
        // equals is based on parent+child only
        assertEquals(rel1, rel2);
    }

    @Test
    public void testCloneProducesEqualObject() {
        PCRelation original = new PCRelation("parentJob", "childJob", true);
        original.setAbstractParentID("ap1");
        original.setAbstractChildID("ac1");
        PCRelation clone = (PCRelation) original.clone();
        assertThat(clone.getParent(), is(original.getParent()));
        assertThat(clone.getChild(), is(original.getChild()));
        assertThat(clone.isDeleted, is(original.isDeleted));
        assertThat(clone.getAbstractParentID(), is(original.getAbstractParentID()));
        assertThat(clone.getAbstractChildID(), is(original.getAbstractChildID()));
    }

    @Test
    public void testCloneIsIndependentObject() {
        PCRelation original = new PCRelation("p", "c");
        PCRelation clone = (PCRelation) original.clone();
        assertNotSame(original, clone);
    }

    @Test
    public void testToStringContainsParentAndChild() {
        PCRelation rel = new PCRelation("parentJob", "childJob");
        String str = rel.toString();
        assertThat(str, containsString("parentJob"));
        assertThat(str, containsString("childJob"));
    }

    @Test
    public void testToStringContainsDeletedFlag() {
        PCRelation rel = new PCRelation("p", "c", true);
        assertThat(rel.toString(), containsString("true"));
    }

    @Test
    public void testSetParentUpdatesParent() {
        PCRelation rel = new PCRelation("old", "c");
        rel.setParent("new");
        assertThat(rel.getParent(), is("new"));
    }

    @Test
    public void testSetChildUpdatesChild() {
        PCRelation rel = new PCRelation("p", "old");
        rel.setChild("new");
        assertThat(rel.getChild(), is("new"));
    }
}
