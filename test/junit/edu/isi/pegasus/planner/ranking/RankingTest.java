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
package edu.isi.pegasus.planner.ranking;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Ranking data class. */
public class RankingTest {

    private Ranking mRanking;

    @BeforeEach
    public void setUp() {
        mRanking = new Ranking("test.dax", 1000L);
    }

    @Test
    public void testConstructorSetsName() {
        assertEquals("test.dax", mRanking.getName(), "Name should be set by constructor");
    }

    @Test
    public void testConstructorSetsRuntime() {
        assertEquals(1000L, mRanking.getRuntime(), "Runtime should be set by constructor");
    }

    @Test
    public void testInitialRankIsZero() {
        assertEquals(0L, mRanking.getRank(), "Initial rank should be 0");
    }

    @Test
    public void testSetRank() {
        mRanking.setRank(500L);
        assertEquals(500L, mRanking.getRank(), "Rank should be updated by setRank");
    }

    @Test
    public void testSetName() {
        mRanking.setName("updated.dax");
        assertEquals("updated.dax", mRanking.getName(), "Name should be updated by setName");
    }

    @Test
    public void testSetRuntime() {
        mRanking.setRuntime(2000L);
        assertEquals(2000L, mRanking.getRuntime(), "Runtime should be updated by setRuntime");
    }

    @Test
    public void testToStringContainsNameAndRank() {
        mRanking.setRank(500L);
        String str = mRanking.toString();
        assertTrue(str.contains("test.dax"), "toString should contain the DAX name");
        assertTrue(str.contains("500"), "toString should contain the rank value");
    }

    @Test
    public void testEqualsWithSameValues() {
        Ranking other = new Ranking("test.dax", 1000L);
        other.setRank(0L);
        assertTrue(mRanking.equals(other), "Rankings with same name and rank should be equal");
    }

    @Test
    public void testNotEqualWithDifferentName() {
        Ranking other = new Ranking("other.dax", 1000L);
        assertFalse(mRanking.equals(other), "Rankings with different names should not be equal");
    }

    @Test
    public void testCompareTo() {
        Ranking r1 = new Ranking("dax1", 1000L);
        r1.setRank(100L);
        Ranking r2 = new Ranking("dax2", 2000L);
        r2.setRank(200L);
        // r1.rank (100) < r2.rank (200), so compareTo should return negative
        assertTrue(r1.compareTo(r2) < 0, "Lower rank should compare less");
    }

    @Test
    public void testCompareToWithEqualRanks() {
        Ranking r1 = new Ranking("dax1", 1000L);
        r1.setRank(100L);
        Ranking r2 = new Ranking("dax2", 2000L);
        r2.setRank(100L);
        assertEquals(0, r1.compareTo(r2), "Equal ranks should compare to zero");
    }

    @Test
    public void testSortingBehavior() {
        List<Ranking> rankings = new ArrayList<>();
        Ranking r1 = new Ranking("dax1", 1000L);
        r1.setRank(100L);
        Ranking r2 = new Ranking("dax2", 2000L);
        r2.setRank(300L);
        Ranking r3 = new Ranking("dax3", 3000L);
        r3.setRank(200L);

        rankings.add(r1);
        rankings.add(r2);
        rankings.add(r3);

        Collections.sort(rankings);
        assertEquals("dax1", rankings.get(0).getName(), "Lowest rank should be first after sort");
    }
}
