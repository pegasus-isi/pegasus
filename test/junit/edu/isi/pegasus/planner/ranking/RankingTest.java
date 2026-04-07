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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
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
        assertThat(mRanking.getName(), is("test.dax"));
    }

    @Test
    public void testConstructorSetsRuntime() {
        assertThat(mRanking.getRuntime(), is(1000L));
    }

    @Test
    public void testInitialRankIsZero() {
        assertThat(mRanking.getRank(), is(0L));
    }

    @Test
    public void testSetRank() {
        mRanking.setRank(500L);
        assertThat(mRanking.getRank(), is(500L));
    }

    @Test
    public void testSetName() {
        mRanking.setName("updated.dax");
        assertThat(mRanking.getName(), is("updated.dax"));
    }

    @Test
    public void testSetRuntime() {
        mRanking.setRuntime(2000L);
        assertThat(mRanking.getRuntime(), is(2000L));
    }

    @Test
    public void testToStringContainsNameAndRank() {
        mRanking.setRank(500L);
        String str = mRanking.toString();
        assertThat(str, allOf(containsString("test.dax"), containsString("500")));
    }

    @Test
    public void testEqualsWithSameValues() {
        Ranking other = new Ranking("test.dax", 1000L);
        other.setRank(0L);
        assertThat(mRanking.equals(other), is(true));
    }

    @Test
    public void testNotEqualWithDifferentName() {
        Ranking other = new Ranking("other.dax", 1000L);
        assertThat(mRanking.equals(other), is(false));
    }

    @Test
    public void testCompareTo() {
        Ranking r1 = new Ranking("dax1", 1000L);
        r1.setRank(100L);
        Ranking r2 = new Ranking("dax2", 2000L);
        r2.setRank(200L);
        // r1.rank (100) < r2.rank (200), so compareTo should return negative
        assertThat(r1.compareTo(r2) < 0, is(true));
    }

    @Test
    public void testCompareToWithEqualRanks() {
        Ranking r1 = new Ranking("dax1", 1000L);
        r1.setRank(100L);
        Ranking r2 = new Ranking("dax2", 2000L);
        r2.setRank(100L);
        assertThat(r1.compareTo(r2), is(0));
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
        assertThat(rankings.get(0).getName(), is("dax1"));
    }

    @Test
    public void testToStringUsesTabbedNameRankRuntimeFormat() {
        mRanking.setRank(500L);
        assertThat(mRanking.toString(), is("test.dax\t500\t1000"));
    }

    @Test
    public void testEqualsIgnoresRuntimeWhenNameAndRankMatch() {
        mRanking.setRank(10L);
        Ranking other = new Ranking("test.dax", 9999L);
        other.setRank(10L);
        assertThat(mRanking.equals(other), is(true));
    }

    @Test
    public void testEqualsReturnsFalseForNull() {
        assertThat(mRanking.equals(null), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForDifferentType() {
        assertThat(mRanking.equals("not-a-ranking"), is(false));
    }
}
