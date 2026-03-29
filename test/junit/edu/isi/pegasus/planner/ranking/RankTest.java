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

import org.junit.jupiter.api.Test;

/** Tests for the Rank class (tests initialization and basic properties). */
public class RankTest {

    @Test
    public void testDefaultConstructor() {
        Rank rank = new Rank();
        assertNotNull(rank, "Rank should be instantiatable with no-arg constructor");
    }

    @Test
    public void testRankCanBeInstantiatedWithDefaultConstructor() {
        assertDoesNotThrow(Rank::new, "Rank should not throw during construction");
    }

    @Test
    public void testRankIsCorrectType() {
        Rank rank = new Rank();
        assertInstanceOf(Rank.class, rank, "Object should be an instance of Rank");
    }

    @Test
    public void testRankingClassConstructor() {
        Ranking ranking = new Ranking("test.dax", 1000L);
        assertNotNull(ranking, "Ranking should be constructable");
        assertEquals("test.dax", ranking.getName());
        assertEquals(1000L, ranking.getRuntime());
    }

    @Test
    public void testRankingInitialRank() {
        Ranking ranking = new Ranking("test.dax", 0L);
        assertEquals(0L, ranking.getRank(), "Initial rank should be 0");
    }

    @Test
    public void testRankingComparableImplementation() {
        Ranking r1 = new Ranking("a.dax", 100L);
        r1.setRank(10L);
        Ranking r2 = new Ranking("b.dax", 200L);
        r2.setRank(20L);

        assertInstanceOf(Comparable.class, r1, "Ranking should implement Comparable");
        assertTrue(r1.compareTo(r2) < 0, "r1 with lower rank should compare less than r2");
        assertTrue(r2.compareTo(r1) > 0, "r2 with higher rank should compare greater than r1");
    }

    @Test
    public void testRankingCompareToThrowsForWrongType() {
        Ranking r = new Ranking("test.dax", 100L);
        assertThrows(
                ClassCastException.class,
                () -> r.compareTo("not a ranking"),
                "compareTo should throw ClassCastException for non-Ranking object");
    }
}
