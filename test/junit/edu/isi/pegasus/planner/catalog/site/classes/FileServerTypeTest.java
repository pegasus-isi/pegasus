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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/**
 * Tests for FileServerType via FileServer (concrete subclass) and for the OPERATION enum.
 *
 * @author Rajiv Mayani
 */
public class FileServerTypeTest {

    @Test
    public void testDefaultConstructorDefaults() {
        FileServer fs = new FileServer();
        assertThat(fs.getProtocol(), is(""));
        assertThat(fs.getURLPrefix(), is(""));
        assertThat(fs.getMountPoint(), is(""));
        assertThat(fs.getSupportedOperation(), is(FileServerType.OPERATION.all));
    }

    @Test
    public void testOverloadedConstructorSetsFields() {
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        assertThat(fs.getProtocol(), is("gsiftp"));
        assertThat(fs.getURLPrefix(), is("gsiftp://site.edu"));
        assertThat(fs.getMountPoint(), is("/data"));
    }

    @Test
    public void testSetAndGetProtocol() {
        FileServer fs = new FileServer();
        fs.setProtocol("scp");
        assertThat(fs.getProtocol(), is("scp"));
    }

    @Test
    public void testSetAndGetURLPrefix() {
        FileServer fs = new FileServer();
        fs.setURLPrefix("http://storage.example.org");
        assertThat(fs.getURLPrefix(), is("http://storage.example.org"));
    }

    @Test
    public void testSetAndGetMountPoint() {
        FileServer fs = new FileServer();
        fs.setMountPoint("/lustre/scratch");
        assertThat(fs.getMountPoint(), is("/lustre/scratch"));
    }

    @Test
    public void testSetSupportedOperationByEnum() {
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.put);
        assertThat(fs.getSupportedOperation(), is(FileServerType.OPERATION.put));
    }

    @Test
    public void testSetSupportedOperationByString() {
        FileServer fs = new FileServer();
        fs.setSupportedOperation("get");
        assertThat(fs.getSupportedOperation(), is(FileServerType.OPERATION.get));
    }

    @Test
    public void testSetSupportedOperationByInvalidStringThrows() {
        FileServer fs = new FileServer();

        assertThrows(IllegalArgumentException.class, () -> fs.setSupportedOperation("GET"));
    }

    @Test
    public void testOperationEnumValues() {
        FileServerType.OPERATION[] ops = FileServerType.OPERATION.values();
        assertThat(ops.length, is(3));
    }

    @Test
    public void testOperationsForGETContainsGetAndAll() {
        Collection<FileServerType.OPERATION> ops = FileServerType.OPERATION.operationsForGET();
        assertThat(ops, hasItem(FileServerType.OPERATION.get));
        assertThat(ops, hasItem(FileServerType.OPERATION.all));
    }

    @Test
    public void testOperationsForPUTContainsPutAndAll() {
        Collection<FileServerType.OPERATION> ops = FileServerType.OPERATION.operationsForPUT();
        assertThat(ops, hasItem(FileServerType.OPERATION.put));
        assertThat(ops, hasItem(FileServerType.OPERATION.all));
    }

    @Test
    public void testOperationsForGetReturnsGetAndAllOperations() {
        Collection<FileServerType.OPERATION> ops =
                FileServerType.OPERATION.operationsFor(FileServerType.OPERATION.get);

        assertThat(ops, hasItem(FileServerType.OPERATION.get));
        assertThat(ops, hasItem(FileServerType.OPERATION.all));
        assertThat(ops.contains(FileServerType.OPERATION.put), is(false));
    }

    @Test
    public void testOperationsForPutReturnsPutAndAllOperations() {
        Collection<FileServerType.OPERATION> ops =
                FileServerType.OPERATION.operationsFor(FileServerType.OPERATION.put);

        assertThat(ops, hasItem(FileServerType.OPERATION.put));
        assertThat(ops, hasItem(FileServerType.OPERATION.all));
        assertThat(ops.contains(FileServerType.OPERATION.get), is(false));
    }

    @Test
    public void testAddProfileStoresProfile() {
        FileServer fs = new FileServer();
        Profile profile = new Profile(Profile.ENV, "PATH", "/bin");

        fs.addProfile(profile);

        assertThat(fs.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(fs.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testSetProfilesReplacesExistingProfiles() {
        FileServer fs = new FileServer();
        fs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        Profiles replacement = new Profiles();
        replacement.addProfileDirectly(Profiles.NAMESPACES.globus, "maxwalltime", "60");
        fs.setProfiles(replacement);

        assertThat(fs.getProfiles().getProfiles(Profile.ENV).isEmpty(), is(true));
        assertThat(fs.getProfiles().getProfiles(Profile.GLOBUS).size(), is(1));
        assertThat(
                fs.getProfiles().getProfiles(Profile.GLOBUS).get(0).getProfileKey(),
                is("maxwalltime"));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServerType.OPERATION.put);
        FileServer cloned = (FileServer) fs.clone();
        assertNotSame(fs, cloned);
        assertThat(cloned.getProtocol(), is(fs.getProtocol()));
        assertThat(cloned.getURLPrefix(), is(fs.getURLPrefix()));
        assertThat(cloned.getMountPoint(), is(fs.getMountPoint()));
        assertThat(cloned.getSupportedOperation(), is(fs.getSupportedOperation()));
    }

    @Test
    public void testCloneDeepCopiesProfiles() {
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        FileServer cloned = (FileServer) fs.clone();

        assertNotSame(fs.getProfiles(), cloned.getProfiles());
        fs.addProfile(new Profile(Profile.ENV, "HOME", "/home/user"));

        assertThat(fs.getProfiles().getProfiles(Profile.ENV).size(), is(2));
        assertThat(cloned.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(
                cloned.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }
}
