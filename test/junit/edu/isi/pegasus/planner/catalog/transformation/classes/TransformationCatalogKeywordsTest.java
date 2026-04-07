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
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for the TransformationCatalogKeywords enum. */
public class TransformationCatalogKeywordsTest {

    @Test
    public void testGetReservedNameForPegasus() {
        assertThat(TransformationCatalogKeywords.PEGASUS.getReservedName(), is("pegasus"));
    }

    @Test
    public void testGetReservedNameForNamespace() {
        assertThat(TransformationCatalogKeywords.NAMESPACE.getReservedName(), is("namespace"));
    }

    @Test
    public void testGetReservedNameForName() {
        assertThat(TransformationCatalogKeywords.NAME.getReservedName(), is("name"));
    }

    @Test
    public void testGetReservedNameForTransformations() {
        assertThat(
                TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(),
                is("transformations"));
    }

    @Test
    public void testGetReservedNameForContainers() {
        assertThat(TransformationCatalogKeywords.CONTAINERS.getReservedName(), is("containers"));
    }

    @Test
    public void testGetReservedNameForSites() {
        assertThat(TransformationCatalogKeywords.SITES.getReservedName(), is("sites"));
    }

    @Test
    public void testGetReservedNameForType() {
        assertThat(TransformationCatalogKeywords.TYPE.getReservedName(), is("type"));
    }

    @Test
    public void testGetReservedNameForPfn() {
        assertThat(TransformationCatalogKeywords.SITE_PFN.getReservedName(), is("pfn"));
    }

    @Test
    public void testGetReservedNameForContainerImage() {
        assertThat(TransformationCatalogKeywords.CONTAINER_IMAGE.getReservedName(), is("image"));
    }

    @Test
    public void testGetReservedKeyLookupByName() {
        assertThat(
                TransformationCatalogKeywords.getReservedKey("name"),
                is(sameInstance(TransformationCatalogKeywords.NAME)));
    }

    @Test
    public void testGetReservedKeyLookupForPegasus() {
        assertThat(
                TransformationCatalogKeywords.getReservedKey("pegasus"),
                is(sameInstance(TransformationCatalogKeywords.PEGASUS)));
    }

    @Test
    public void testGetReservedKeyLookupForTransformations() {
        assertThat(
                TransformationCatalogKeywords.getReservedKey("transformations"),
                is(sameInstance(TransformationCatalogKeywords.TRANSFORMATIONS)));
    }

    @Test
    public void testGetReservedKeyReturnsNullForUnknownKey() {
        assertThat(
                TransformationCatalogKeywords.getReservedKey("unknown-key-xyz"), is(nullValue()));
    }

    @Test
    public void testAllEnumValuesHaveNonEmptyReservedNames() {
        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertThat(
                    "Reserved name should not be null for " + keyword,
                    keyword.getReservedName(),
                    is(notNullValue()));
            assertThat(
                    "Reserved name should not be empty for " + keyword,
                    keyword.getReservedName().isEmpty(),
                    is(false));
        }
    }

    @Test
    public void testGetReservedKeyForAllValues() {
        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertThat(
                    TransformationCatalogKeywords.getReservedKey(keyword.getReservedName()),
                    is(sameInstance(keyword)));
        }
    }

    @Test
    public void testContainerMountKeyword() {
        assertThat(TransformationCatalogKeywords.CONTAINER_MOUNT.getReservedName(), is("mounts"));
    }

    @Test
    public void testChecksumKeyword() {
        assertThat(TransformationCatalogKeywords.CHECKSUM.getReservedName(), is("checksum"));
    }

    @Test
    public void testBypassKeyword() {
        assertThat(TransformationCatalogKeywords.BYPASS.getReservedName(), is("bypass"));
    }

    @Test
    public void testGetReservedKeyIsCaseSensitive() {
        assertThat(TransformationCatalogKeywords.getReservedKey("PEGASUS"), is(nullValue()));
        assertThat(TransformationCatalogKeywords.getReservedKey("Name"), is(nullValue()));
    }

    @Test
    public void testAllReservedNamesAreUnique() {
        Set<String> reservedNames = new HashSet<>();

        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertThat(reservedNames.add(keyword.getReservedName()), is(true));
        }
    }

    @Test
    public void testValueOfMatchesDeclaredEnumNames() {
        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertThat(
                    TransformationCatalogKeywords.valueOf(keyword.name()),
                    is(sameInstance(keyword)));
        }
    }
}
