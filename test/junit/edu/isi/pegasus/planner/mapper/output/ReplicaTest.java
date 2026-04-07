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
package edu.isi.pegasus.planner.mapper.output;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the Replica output mapper class structure. */
public class ReplicaTest {

    @Test
    public void testReplicaImplementsOutputMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                OutputMapper.class.isAssignableFrom(Replica.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testPropertyPrefixConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Replica.PROPERTY_PREFIX,
                org.hamcrest.Matchers.is("pegasus.dir.storage.mapper.replica"));
    }

    @Test
    public void testDefaultInstantiation() {
        Replica replica = new Replica();
        org.hamcrest.MatcherAssert.assertThat(replica, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testReplicaIsPublicClass() {
        int modifiers = Replica.class.getModifiers();
        org.hamcrest.MatcherAssert.assertThat(
                java.lang.reflect.Modifier.isPublic(modifiers), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testReplicaDoesNotExtendAbstractFileFactoryBasedMapper() {
        // Replica implements OutputMapper directly
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Replica.class),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Replica().description(), org.hamcrest.Matchers.is("Replica Catalog Mapper"));
    }

    @Test
    public void testGetErrorMessagePrefixUsesShortName() {
        org.hamcrest.MatcherAssert.assertThat(
                new TestReplica().errorPrefix(), org.hamcrest.Matchers.is("[Replica] "));
    }

    @Test
    public void testMapWithoutSiteUsesFirstReplicaCatalogEntry() throws Exception {
        TestReplica replica = new TestReplica();
        replica.mRCCatalog =
                replicaCatalogFor(
                        Collections.singletonList(
                                new ReplicaCatalogEntry("file:///data/a", "local")),
                        null);

        NameValue<String, String> result = replica.map("lfn", null, FileServer.OPERATION.put);

        org.hamcrest.MatcherAssert.assertThat(result, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(result.getKey(), org.hamcrest.Matchers.is("local"));
        org.hamcrest.MatcherAssert.assertThat(
                result.getValue(), org.hamcrest.Matchers.is("file:///data/a"));
    }

    @Test
    public void testMapWithSiteUsesSiteSpecificLookup() throws Exception {
        TestReplica replica = new TestReplica();
        replica.mRCCatalog = replicaCatalogFor(null, "gsiftp://example.org/work/lfn");

        NameValue<String, String> result = replica.map("lfn", "isi", FileServer.OPERATION.get);

        org.hamcrest.MatcherAssert.assertThat(result, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(result.getKey(), org.hamcrest.Matchers.is("isi"));
        org.hamcrest.MatcherAssert.assertThat(
                result.getValue(), org.hamcrest.Matchers.is("gsiftp://example.org/work/lfn"));
    }

    @Test
    public void testMapAllFiltersReplicaEntriesByRequestedSite() throws Exception {
        TestReplica replica = new TestReplica();
        replica.mRCCatalog =
                replicaCatalogFor(
                        Arrays.asList(
                                new ReplicaCatalogEntry("file:///data/a", "local"),
                                new ReplicaCatalogEntry("file:///data/b", "condorpool")),
                        null);

        List<NameValue<String, String>> result =
                replica.mapAll("lfn", "condorpool", FileServer.OPERATION.put);

        org.hamcrest.MatcherAssert.assertThat(result, org.hamcrest.Matchers.notNullValue());
        org.hamcrest.MatcherAssert.assertThat(result.size(), org.hamcrest.Matchers.is(1));
        org.hamcrest.MatcherAssert.assertThat(
                result.get(0).getKey(), org.hamcrest.Matchers.is("condorpool"));
        org.hamcrest.MatcherAssert.assertThat(
                result.get(0).getValue(), org.hamcrest.Matchers.is("file:///data/b"));
    }

    @Test
    public void testMapThrowsWhenReplicaNotFoundAndExceptionsEnabled() {
        TestReplica replica = new TestReplica();
        replica.mRCCatalog = replicaCatalogFor(null, null);
        replica.mThrowExceptionInCaseOfReplicaNotFound = true;

        MapperException e =
                assertThrows(
                        MapperException.class,
                        () -> replica.map("missing", "local", FileServer.OPERATION.put));

        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage()
                        .contains(
                                "[Replica] Unable to retrieve location from Mapper Replica Backend"),
                org.hamcrest.Matchers.is(true));
    }

    private ReplicaCatalog replicaCatalogFor(
            Collection<ReplicaCatalogEntry> entries, String siteSpecificLookup) {
        return (ReplicaCatalog)
                Proxy.newProxyInstance(
                        ReplicaCatalog.class.getClassLoader(),
                        new Class<?>[] {ReplicaCatalog.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "lookup":
                                    if (args.length == 1) {
                                        return entries;
                                    }
                                    if (args.length == 2) {
                                        return siteSpecificLookup;
                                    }
                                    throw new UnsupportedOperationException(method.getName());
                                case "connect":
                                    return true;
                                case "close":
                                    return null;
                                case "isClosed":
                                    return false;
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }

    private static final class TestReplica extends Replica {

        String errorPrefix() {
            return this.getErrorMessagePrefix();
        }
    }
}
