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
package edu.isi.pegasus.planner.catalog.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.namespace.Metadata;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogEntryTest {

    // --- Constructor tests ---

    @Test
    public void testDefaultConstructorHasNullPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertThat(rce.getPFN(), is(nullValue()));
    }

    @Test
    public void testDefaultConstructorHasNoAttributes() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertThat(rce.getAttributeCount(), is(0));
    }

    @Test
    public void testPfnConstructorSetsPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file");
        assertThat(rce.getPFN(), is("gsiftp://host/file"));
    }

    @Test
    public void testPfnConstructorHasNoResourceHandle() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file");
        assertThat(rce.getResourceHandle(), is(nullValue()));
    }

    @Test
    public void testPfnHandleConstructorSetsBoth() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file", "local");
        assertThat(rce.getPFN(), is("gsiftp://host/file"));
        assertThat(rce.getResourceHandle(), is("local"));
    }

    @Test
    public void testPfnMapConstructorSetsAttributes() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "isi");
        attrs.put("checksum", "abc123");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/file", attrs);
        assertThat(rce.getResourceHandle(), is("isi"));
        assertThat(rce.getAttribute("checksum"), is("abc123"));
    }

    // --- PFN set/get tests ---

    @Test
    public void testSetAndGetPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        rce.setPFN("file:///data/file.txt");
        assertThat(rce.getPFN(), is("file:///data/file.txt"));
    }

    // --- Attribute tests ---

    @Test
    public void testAddAttributeAndGet() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("mykey", "myvalue");
        assertThat(rce.getAttribute("mykey"), is("myvalue"));
    }

    @Test
    public void testHasAttributeReturnsTrueForKnownKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        assertThat(rce.hasAttribute(ReplicaCatalogEntry.RESOURCE_HANDLE), is(true));
    }

    @Test
    public void testHasAttributeReturnsFalseForUnknownKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.hasAttribute("nonexistent"), is(false));
    }

    @Test
    public void testGetAttributeCountAfterAdding() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("extra", "value");
        assertThat(rce.getAttributeCount(), is(2));
    }

    @Test
    public void testSetAttributeOverwritesExisting() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("key", "original");
        rce.setAttribute("key", "updated");
        assertThat(rce.getAttribute("key"), is("updated"));
    }

    @Test
    public void testRemoveAttributeDeletesKey() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute("key", "value");
        Object removed = rce.removeAttribute("key");
        assertThat(removed, is("value"));
        assertThat(rce.hasAttribute("key"), is(false));
    }

    @Test
    public void testRemoveAllAttributeClearsMap() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("extra", "value");
        rce.removeAllAttribute();
        assertThat(rce.getAttributeCount(), is(0));
    }

    // --- Resource handle tests ---

    @Test
    public void testSetResourceHandle() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.setResourceHandle("cluster1");
        assertThat(rce.getResourceHandle(), is("cluster1"));
    }

    @Test
    public void testSetResourceHandleNullDoesNothing() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "existing");
        rce.setResourceHandle(null);
        assertThat(rce.getResourceHandle(), is("existing"));
    }

    // --- Constants tests ---

    @Test
    public void testResourceHandleConstant() {
        assertThat(ReplicaCatalogEntry.RESOURCE_HANDLE, is("site"));
    }

    @Test
    public void testDeprecatedResourceHandleConstant() {
        assertThat(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, is("pool"));
    }

    @Test
    public void testRegexKeyConstant() {
        assertThat(ReplicaCatalogEntry.REGEX_KEY, is("regex"));
    }

    // --- isRegex tests ---

    @Test
    public void testIsRegexReturnsFalseByDefault() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.isRegex(), is(false));
    }

    @Test
    public void testIsRegexReturnsTrueWhenSet() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute(ReplicaCatalogEntry.REGEX_KEY, "true");
        assertThat(rce.isRegex(), is(true));
    }

    // --- equals tests ---

    @Test
    public void testEqualsIdenticalEntries() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test", "site1");
        assertThat(rce1, is(rce2));
    }

    @Test
    public void testEqualsDifferentPFN() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test1", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test2", "site1");
        assertNotEquals(rce1, rce2);
    }

    @Test
    public void testEqualsNullReturnsFalse() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.equals(null), is(false));
    }

    @Test
    public void testEqualsNonRCEReturnsFalse() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.equals("not an RCE"), is(false));
    }

    @Test
    public void testEqualsBothNullPFNs() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry();
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry();
        assertThat(rce1, is(rce2));
    }

    // --- clone test ---

    @Test
    public void testCloneProducesEqualButIndependentCopy() {
        ReplicaCatalogEntry original = new ReplicaCatalogEntry("pfn://test", "site1");
        original.addAttribute("extra", "data");

        ReplicaCatalogEntry clone = (ReplicaCatalogEntry) original.clone();
        assertThat(clone, is(original));

        // modify clone should not affect original
        clone.addAttribute("new_key", "new_val");
        assertThat(original.hasAttribute("new_key"), is(false));
    }

    // --- merge tests ---

    @Test
    public void testMergeWithMatchingPFNReturnsTrue() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test");
        rce2.addAttribute("extra", "value");

        boolean merged = rce1.merge(rce2, false);
        assertThat(merged, is(true));
        assertThat(rce1.hasAttribute("extra"), is(true));
    }

    @Test
    public void testMergeWithMismatchedPFNReturnsFalse() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test1", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test2");

        boolean merged = rce1.merge(rce2, false);
        assertThat(merged, is(false));
    }

    @Test
    public void testStaticMergeWithMatchingPFNs() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test");
        b.addAttribute("extra", "value");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);
        assertThat(merged, is(notNullValue()));
        assertThat(merged.getPFN(), is("pfn://test"));
    }

    @Test
    public void testStaticMergeWithMismatchedPFNsReturnsNull() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test1");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test2");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);
        assertThat(merged, is(nullValue()));
    }

    // --- pool attribute migration test ---

    @Test
    public void testPoolAttributeConvertedToSite() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolsite");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", attrs);
        assertThat(rce.getResourceHandle(), is("poolsite"));
        assertThat(rce.hasAttribute(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE), is(false));
    }

    @Test
    public void testBothSiteAndPoolAttributeThrowsException() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "site1");
        attrs.put(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolsite");
        assertThrows(RuntimeException.class, () -> new ReplicaCatalogEntry("pfn://test", attrs));
    }

    // --- toString test ---

    @Test
    public void testToStringContainsPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/data/file");
        assertThat(rce.toString(), containsString("gsiftp://host/data/file"));
    }

    @Test
    public void testToStringWithNullPFN() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        assertThat(rce.toString(), containsString("(null)"));
    }

    @Test
    public void testToStringIncludesAttributeData() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        assertThat(rce.toString(), containsString("site"));
        assertThat(rce.toString(), containsString("site1"));
    }

    // --- addAttribute(Map) bulk overload ---

    @Test
    public void testAddAttributeMapBulkAddsAll() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        Map<String, Object> extras = new HashMap<>();
        extras.put("k1", "v1");
        extras.put("k2", "v2");
        rce.addAttribute(extras);
        assertThat(rce.getAttribute("k1"), is("v1"));
        assertThat(rce.getAttribute("k2"), is("v2"));
        assertThat(rce.getAttributeCount(), is(2));
    }

    @Test
    public void testAddAttributeMapDoesNotClearExisting() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        Map<String, Object> extras = new HashMap<>();
        extras.put("extra", "val");
        rce.addAttribute(extras);
        // site key should still be present
        assertThat(rce.getResourceHandle(), is("site1"));
        assertThat(rce.getAttributeCount(), is(2));
    }

    // --- addAttribute(Metadata) overload ---

    @Test
    public void testAddAttributeMetadataAddsAllKeys() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        Metadata m = new Metadata();
        m.construct("checksum.type", "sha256");
        m.construct("checksum.value", "abc123");
        rce.addAttribute(m);
        assertThat(rce.getAttribute("checksum.type"), is("sha256"));
        assertThat(rce.getAttribute("checksum.value"), is("abc123"));
    }

    @Test
    public void testAddAttributeEmptyMetadataChangesNothing() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute(new Metadata());
        assertThat(rce.getAttributeCount(), is(1));
    }

    // --- setAttribute(Map) replace-all variant ---

    @Test
    public void testSetAttributeMapReplacesAllExisting() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("old_key", "old_val");

        Map<String, Object> replacement = new HashMap<>();
        replacement.put("new_key", "new_val");
        rce.setAttribute(replacement);

        assertThat(rce.getAttributeCount(), is(1));
        assertThat(rce.getAttribute("new_key"), is("new_val"));
        assertThat(rce.getAttribute("old_key"), is(nullValue()));
        assertThat(rce.getResourceHandle(), is(nullValue())); // site key was cleared
    }

    @Test
    public void testSetAttributeEmptyMapClearsAll() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.setAttribute(new HashMap<>());
        assertThat(rce.getAttributeCount(), is(0));
    }

    // --- getAttributeIterator ---

    @Test
    public void testGetAttributeIteratorCoversAllKeys() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", "site1");
        rce.addAttribute("extra", "val");

        java.util.Set<String> keys = new java.util.HashSet<>();
        for (Iterator it = rce.getAttributeIterator(); it.hasNext(); ) {
            keys.add((String) it.next());
        }
        assertThat(keys, hasItem(ReplicaCatalogEntry.RESOURCE_HANDLE));
        assertThat(keys, hasItem("extra"));
        assertThat(keys.size(), is(2));
    }

    @Test
    public void testGetAttributeIteratorOnEmptyHasNoElements() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.getAttributeIterator().hasNext(), is(false));
    }

    // --- isRegex — false branch ---

    @Test
    public void testIsRegexReturnsFalseWhenSetToFalseString() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute(ReplicaCatalogEntry.REGEX_KEY, "false");
        assertThat(rce.isRegex(), is(false));
    }

    // --- getResourceHandle fallback to deprecated 'pool' attribute ---

    @Test
    public void testGetResourceHandleFallsBackToPoolAttribute() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        // Bypass the constructor guard by adding pool directly after construction
        rce.addAttribute(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolsite");
        assertThat(rce.getResourceHandle(), is("poolsite"));
    }

    @Test
    public void testGetResourceHandlePrefersSiteOverPool() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        rce.addAttribute(ReplicaCatalogEntry.RESOURCE_HANDLE, "siteA");
        rce.addAttribute(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE, "poolB");
        assertThat(rce.getResourceHandle(), is("siteA"));
    }

    // --- removeAttribute on non-existent key ---

    @Test
    public void testRemoveAttributeNonExistentKeyReturnsNull() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test");
        assertThat(rce.removeAttribute("no_such_key"), is(nullValue()));
    }

    // --- merge overwrite semantics (instance method) ---

    @Test
    public void testMergeOverwriteTrueReplacesExistingAttribute() {
        ReplicaCatalogEntry base = new ReplicaCatalogEntry("pfn://test");
        base.addAttribute("key", "original");

        ReplicaCatalogEntry other = new ReplicaCatalogEntry("pfn://test");
        other.addAttribute("key", "updated");

        assertThat(base.merge(other, true), is(true));
        assertThat(base.getAttribute("key"), is("updated"));
    }

    @Test
    public void testMergeOverwriteFalseKeepsExistingAttribute() {
        ReplicaCatalogEntry base = new ReplicaCatalogEntry("pfn://test");
        base.addAttribute("key", "original");

        ReplicaCatalogEntry other = new ReplicaCatalogEntry("pfn://test");
        other.addAttribute("key", "updated");

        assertThat(base.merge(other, false), is(true));
        assertThat(base.getAttribute("key"), is("original"));
    }

    @Test
    public void testMergeAddsNewKeyRegardlessOfOverwrite() {
        ReplicaCatalogEntry base = new ReplicaCatalogEntry("pfn://test");
        ReplicaCatalogEntry other = new ReplicaCatalogEntry("pfn://test");
        other.addAttribute("brand_new", "value");

        assertThat(base.merge(other, false), is(true));
        assertThat(base.getAttribute("brand_new"), is("value"));
    }

    // --- merge / static merge with both null PFNs ---

    @Test
    public void testMergeBothNullPFNsReturnsTrue() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry();
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry();
        rce2.addAttribute("key", "val");
        assertThat(rce1.merge(rce2, false), is(true));
        assertThat(rce1.getAttribute("key"), is("val"));
    }

    @Test
    public void testStaticMergeBothNullPFNsReturnsEntry() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry();
        a.addAttribute("a_key", "a_val");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry();
        b.addAttribute("b_key", "b_val");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);
        assertThat(merged, is(notNullValue()));
        assertThat(merged.getPFN(), is(nullValue()));
        assertThat(merged.getAttribute("a_key"), is("a_val"));
        assertThat(merged.getAttribute("b_key"), is("b_val"));
    }

    @Test
    public void testStaticMergeOverwriteTrueReplacesAttribute() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test");
        a.addAttribute("key", "original");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test");
        b.addAttribute("key", "updated");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, true);
        assertThat(merged, is(notNullValue()));
        assertThat(merged.getAttribute("key"), is("updated"));
    }

    // --- equals edge cases ---

    @Test
    public void testEqualsMatchingPFNButDifferentAttributeCountIsFalse() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test", "site1");
        rce2.addAttribute("extra", "val"); // rce2 now has 2 attributes
        assertNotEquals(rce1, rce2);
    }

    @Test
    public void testEqualsMatchingPFNSameCountDifferentValueIsFalse() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test", "site2");
        assertNotEquals(rce1, rce2);
    }

    @Test
    public void testEqualsOneNullPFNOtherNotIsFalse() {
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry(); // null PFN
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("pfn://test");
        assertNotEquals(rce1, rce2);
    }

    // --- clone with null PFN ---

    @Test
    public void testCloneWithNullPFNProducesEqualEntry() {
        ReplicaCatalogEntry original = new ReplicaCatalogEntry();
        original.addAttribute("key", "val");
        ReplicaCatalogEntry clone = (ReplicaCatalogEntry) original.clone();
        assertThat(clone.getPFN(), is(nullValue()));
        assertThat(clone.getAttribute("key"), is("val"));
    }

    @Test
    public void testMapConstructorDefensivelyCopiesAttributes() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("checksum", "abc123");

        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test", attrs);
        attrs.put("checksum", "mutated");
        attrs.put("new_key", "new_val");

        assertThat(rce.getAttribute("checksum"), is("abc123"));
        assertThat(rce.hasAttribute("new_key"), is(false));
    }

    @Test
    public void testCloneReturnsDistinctInstance() {
        ReplicaCatalogEntry original = new ReplicaCatalogEntry("pfn://test", "site1");

        ReplicaCatalogEntry clone = (ReplicaCatalogEntry) original.clone();

        assertNotSame(original, clone);
    }

    @Test
    public void testStaticMergeReturnsNewEntryInstance() {
        ReplicaCatalogEntry a = new ReplicaCatalogEntry("pfn://test", "site1");
        ReplicaCatalogEntry b = new ReplicaCatalogEntry("pfn://test");

        ReplicaCatalogEntry merged = ReplicaCatalogEntry.merge(a, b, false);

        assertThat(merged, is(notNullValue()));
        assertNotSame(a, merged);
        assertNotSame(b, merged);
    }
}
