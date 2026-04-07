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

import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Rank class (tests initialization and basic properties). */
public class RankTest {

    @Test
    public void testDefaultConstructor() {
        Rank rank = new Rank();
        assertThat(rank, notNullValue());
    }

    @Test
    public void testRankCanBeInstantiatedWithDefaultConstructor() {
        assertDoesNotThrow(Rank::new, "Rank should not throw during construction");
    }

    @Test
    public void testRankIsCorrectType() {
        Rank rank = new Rank();
        assertThat(rank, instanceOf(Rank.class));
    }

    @Test
    public void testRankingClassConstructor() {
        Ranking ranking = new Ranking("test.dax", 1000L);
        assertThat(ranking, notNullValue());
        assertThat(ranking.getName(), is("test.dax"));
        assertThat(ranking.getRuntime(), is(1000L));
    }

    @Test
    public void testRankingInitialRank() {
        Ranking ranking = new Ranking("test.dax", 0L);
        assertThat(ranking.getRank(), is(0L));
    }

    @Test
    public void testRankingComparableImplementation() {
        Ranking r1 = new Ranking("a.dax", 100L);
        r1.setRank(10L);
        Ranking r2 = new Ranking("b.dax", 200L);
        r2.setRank(20L);

        assertThat(r1, instanceOf(Comparable.class));
        assertThat(r1.compareTo(r2) < 0, is(true));
        assertThat(r2.compareTo(r1) > 0, is(true));
    }

    @Test
    public void testRankingCompareToThrowsForWrongType() {
        Ranking r = new Ranking("test.dax", 100L);
        assertThrows(
                ClassCastException.class,
                () -> r.compareTo("not a ranking"),
                "compareTo should throw ClassCastException for non-Ranking object");
    }

    @Test
    public void testRankOnEmptyCollectionReturnsEmptyResultWithoutInitialization() {
        Rank rank = new Rank();
        Collection<Ranking> result = rank.rank(Collections.emptyList());
        assertThat(result, notNullValue());
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void testRankDeclaresExpectedPrivateFields() throws Exception {
        assertThat(Rank.class.getDeclaredFields().length, is(5));
        assertThat(
                (Object) Rank.class.getDeclaredField("mRequestID").getType(),
                is((Object) String.class));
    }

    @Test
    public void testRankDefaultConstructorLeavesRequestIdUnset() throws Exception {
        Rank rank = new Rank();
        assertThat(ReflectionTestUtils.getField(rank, "mRequestID"), nullValue());
    }
}
