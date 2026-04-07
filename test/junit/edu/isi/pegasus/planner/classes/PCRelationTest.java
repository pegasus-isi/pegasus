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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PCRelationTest {

    @Test
    public void testDefaultConstructorInitializesEmptyStrings() {
        PCRelation rel = new PCRelation();
        assertThat(rel.getParent(), is(""));
        assertThat(rel.getChild(), is(""));
    }

    @Test
    public void testDefaultConstructorIsDeletedFalse() {
        PCRelation rel = new PCRelation();
        assertThat(rel.isDeleted, is(false));
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
        assertThat(rel.isDeleted, is(false));
    }

    @Test
    public void testThreeArgConstructorSetsDeletedFlag() {
        PCRelation rel = new PCRelation("parentJob", "childJob", true);
        assertThat(rel.isDeleted, is(true));
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
        assertThat(rel1, is(rel2));
    }

    @Test
    public void testEqualsDifferentParentNotEqual() {
        PCRelation rel1 = new PCRelation("p1", "c");
        PCRelation rel2 = new PCRelation("p2", "c");
        assertThat(rel1, is(not(rel2)));
    }

    @Test
    public void testEqualsDifferentChildNotEqual() {
        PCRelation rel1 = new PCRelation("p", "c1");
        PCRelation rel2 = new PCRelation("p", "c2");
        assertThat(rel1, is(not(rel2)));
    }

    @Test
    public void testEqualityIgnoresDeletedFlag() {
        PCRelation rel1 = new PCRelation("p", "c", true);
        PCRelation rel2 = new PCRelation("p", "c", false);
        // equals is based on parent+child only
        assertThat(rel1, is(rel2));
    }

    @Test
    public void testEqualsWithNullThrowsNullPointerException() {
        PCRelation rel = new PCRelation("p", "c");
        assertThrows(NullPointerException.class, () -> rel.equals(null));
    }

    @Test
    public void testEqualsWithDifferentTypeThrowsClassCastException() {
        PCRelation rel = new PCRelation("p", "c");
        assertThrows(ClassCastException.class, () -> rel.equals("not-a-relation"));
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
        assertThat(clone, is(not(sameInstance(original))));
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

    @Test
    public void testCompareToReturnsZeroForEquivalentRelation() {
        PCRelation rel1 = new PCRelation("p", "c");
        PCRelation rel2 = new PCRelation("p", "c");

        assertThat(rel1.compareTo(rel2), is(0));
    }

    @Test
    public void testCompareToReturnsOneForDifferentRelation() {
        PCRelation rel1 = new PCRelation("p", "c");
        PCRelation rel2 = new PCRelation("p", "other");

        assertThat(rel1.compareTo(rel2), is(1));
    }

    @Test
    public void testToDOTReturnsQuotedEdgeLine() throws IOException {
        PCRelation rel = new PCRelation("parentJob", "childJob");

        assertThat(rel.toDOT(), is("\"parentJob\" -> \"childJob\"" + System.lineSeparator()));
    }

    @Test
    public void testToDOTWriterHonorsIndent() throws IOException {
        PCRelation rel = new PCRelation("parentJob", "childJob");
        StringWriter writer = new StringWriter();

        rel.toDOT(writer, "  ");

        assertThat(
                writer.toString(), is("  \"parentJob\" -> \"childJob\"" + System.lineSeparator()));
    }
}
