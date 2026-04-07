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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteCatalogKeywordsTest {

    @Test
    public void testProfilesKeyReservedName() {
        assertThat(SiteCatalogKeywords.PROFILES.getReservedName(), is("profiles"));
    }

    @Test
    public void testSitesKeyReservedName() {
        assertThat(SiteCatalogKeywords.SITES.getReservedName(), is("sites"));
    }

    @Test
    public void testNameKeyReservedName() {
        assertThat(SiteCatalogKeywords.NAME.getReservedName(), is("name"));
    }

    @Test
    public void testPegasusKeyReservedName() {
        assertThat(SiteCatalogKeywords.PEGASUS.getReservedName(), is("pegasus"));
    }

    @Test
    public void testGetReservedKeyReturnsSitesForSitesString() {
        SiteCatalogKeywords kw = SiteCatalogKeywords.getReservedKey("sites");
        assertThat(kw, is(SiteCatalogKeywords.SITES));
    }

    @Test
    public void testGetReservedKeyReturnsNullForUnknownKey() {
        assertThat(SiteCatalogKeywords.getReservedKey("unknownKey"), is(nullValue()));
    }

    @Test
    public void testValueOfSites() {
        assertThat(SiteCatalogKeywords.valueOf("SITES"), is(SiteCatalogKeywords.SITES));
    }

    @Test
    public void testAllKeywordsHaveNonNullReservedNames() {
        for (SiteCatalogKeywords kw : SiteCatalogKeywords.values()) {
            assertThat(kw.getReservedName(), is(notNullValue()));
        }
    }

    @Test
    public void testGetReservedKeyForOperation() {
        assertThat(
                SiteCatalogKeywords.getReservedKey("operation"), is(SiteCatalogKeywords.OPERATION));
    }

    @Test
    public void testGetReservedKeyForFileServers() {
        assertThat(
                SiteCatalogKeywords.getReservedKey("fileServers"),
                is(SiteCatalogKeywords.FILESERVERS));
    }

    @Test
    public void testEveryKeywordRoundTripsThroughReservedLookup() {
        for (SiteCatalogKeywords keyword : SiteCatalogKeywords.values()) {
            assertThat(SiteCatalogKeywords.getReservedKey(keyword.getReservedName()), is(keyword));
        }
    }

    @Test
    public void testReservedLookupIsCaseSensitive() {
        assertThat(SiteCatalogKeywords.getReservedKey("Sites"), is(nullValue()));
        assertThat(SiteCatalogKeywords.getReservedKey("FILESERVERS"), is(nullValue()));
        assertThat(SiteCatalogKeywords.getReservedKey("Operation"), is(nullValue()));
    }

    @Test
    public void testValueOfStillResolvesEnumConstantNames() {
        for (SiteCatalogKeywords keyword : SiteCatalogKeywords.values()) {
            assertThat(SiteCatalogKeywords.valueOf(keyword.name()), is(keyword));
        }
    }
}
