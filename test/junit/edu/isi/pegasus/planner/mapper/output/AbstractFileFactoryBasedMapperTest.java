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

import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.griphyn.vdl.euryale.FileFactory;
import org.junit.jupiter.api.Test;

/** Tests for the AbstractFileFactoryBasedMapper class structure. */
public class AbstractFileFactoryBasedMapperTest {

    @Test
    public void testImplementsOutputMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                OutputMapper.class.isAssignableFrom(AbstractFileFactoryBasedMapper.class),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Flat.class),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testHashedExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Hashed.class),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatInstantiation() {
        Flat flat = new Flat();
        org.hamcrest.MatcherAssert.assertThat(flat, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testHashedInstantiation() {
        Hashed hashed = new Hashed();
        org.hamcrest.MatcherAssert.assertThat(hashed, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testGetErrorMessagePrefixUsesShortName() {
        TestMapper mapper = new TestMapper();

        org.hamcrest.MatcherAssert.assertThat(
                mapper.errorPrefix(), org.hamcrest.Matchers.is("[test] "));
    }

    @Test
    public void testConstructURLAddsSeparatorWhenAddonIsRelative() {
        TestMapper mapper = new TestMapper();
        FileServer server = new FileServer("file", "file:///tmp", "/storage");

        NameValue<String, String> url = mapper.construct("local", server, "outputs/f.a");

        org.hamcrest.MatcherAssert.assertThat(url.getKey(), org.hamcrest.Matchers.is("local"));
        org.hamcrest.MatcherAssert.assertThat(
                url.getValue(), org.hamcrest.Matchers.is("file:///tmp/storage/outputs/f.a"));
    }

    @Test
    public void testLookupStorageDirectoryThrowsWhenSiteMissing() {
        TestMapper mapper = new TestMapper();
        mapper.mSiteStore = new SiteStore();

        MapperException exception =
                assertThrows(MapperException.class, () -> mapper.lookup("missing"));

        org.hamcrest.MatcherAssert.assertThat(
                exception.getMessage(),
                org.hamcrest.Matchers.is("[test] Unable to lookup site catalog for site missing"));
    }

    @Test
    public void testComplainForStorageFileServerThrowsExpectedMessage() {
        TestMapper mapper = new TestMapper();

        MapperException exception =
                assertThrows(
                        MapperException.class,
                        () -> mapper.complain(FileServer.OPERATION.put, "local"));

        org.hamcrest.MatcherAssert.assertThat(
                exception.getMessage(),
                org.hamcrest.Matchers.is(
                        "[test]  File Server not specified for shared-storage filesystem for operation put against site: local"));
    }

    @Test
    public void testMapAllReturnsUrlsForAllMatchingFileServers() throws Exception {
        TestMapper mapper = new TestMapper();
        mapper.mStageoutDirectoriesStore = new HashMap<String, Directory>();
        mapper.mStageoutDirectoriesStore.put("local", createDirectoryWithServers());

        List<NameValue<String, String>> urls =
                mapper.mapAll("f.a", "local", FileServer.OPERATION.get);

        org.hamcrest.MatcherAssert.assertThat(urls.size(), org.hamcrest.Matchers.is(2));
        Set<String> values = new HashSet<String>();
        for (NameValue<String, String> url : urls) {
            values.add(url.getValue());
        }
        org.hamcrest.MatcherAssert.assertThat(
                values,
                org.hamcrest.Matchers.is(
                        new HashSet<String>(
                                java.util.Arrays.asList(
                                        "file:///tmp/storage/f.a",
                                        "gsiftp://example.com/storage/f.a"))));
    }

    private static Directory createDirectoryWithServers() {
        Directory directory = new Directory();
        directory.setType(Directory.TYPE.shared_storage);
        directory.setInternalMountPoint(new InternalMountPoint("/storage"));

        FileServer allServer = new FileServer("file", "file:///tmp", "/storage");
        allServer.setSupportedOperation(FileServer.OPERATION.all);
        directory.addFileServer(allServer);

        FileServer getServer = new FileServer("gsiftp", "gsiftp://example.com", "/storage");
        getServer.setSupportedOperation(FileServer.OPERATION.get);
        directory.addFileServer(getServer);

        return directory;
    }

    private static class TestMapper extends AbstractFileFactoryBasedMapper {
        @Override
        public String description() {
            return "tracking mapper";
        }

        @Override
        public FileFactory instantiateFileFactory(PegasusBag bag, ADag workflow) {
            return null;
        }

        @Override
        public String getShortName() {
            return "test";
        }

        @Override
        public String createAndGetAddOn(String lfn, String site, boolean existing) {
            return lfn;
        }

        String errorPrefix() {
            return getErrorMessagePrefix();
        }

        NameValue<String, String> construct(String site, FileServer server, String addOn)
                throws MapperException {
            return constructURL(site, server, addOn);
        }

        Directory lookup(String site) throws MapperException {
            return lookupStorageDirectory(site);
        }

        void complain(FileServer.OPERATION operation, String site) {
            complainForStorageFileServer(operation, site);
        }
    }
}
