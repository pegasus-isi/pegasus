/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container.MountPoint;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/** @author vahi */
public class ContainerTest {

    private TestSetup mTestSetup;

    public ContainerTest() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterClass
    public static void tearDownClass() {}

    @After
    public void tearDown() {}

    @Test
    public void testSingularityFileCVMFS() {
        this.testSingulartiy(
                "test",
                "test",
                "file:///cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:latest");
    }

    @Test
    public void testSingularityFileCVMFSTagVersion() {
        this.testSingulartiy(
                "test",
                "test",
                "file://localhost/cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:latest");
    }

    @Test
    public void testSingularityFileCVMFSTagVersionWithDot() {
        this.testSingulartiy(
                "test",
                "test",
                "file://localhost/cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:v1.13.0");
    }

    @Test
    public void testSingulartiyHUB() {
        this.testSingulartiy("test", "test.simg", "shub://pegasus-isi/montage-workflow-v2");
    }

    @Test
    public void testSingulartiyHTTPImg() {
        this.testSingulartiy(
                "test", "test.img", "http:///pegasus.isi.edu/images/singularity/centos-7.img");
    }

    @Test
    public void testSingulartiyHTTPSImg() {
        this.testSingulartiy(
                "test", "test.simg", "http:///pegasus.isi.edu/images/singularity/centos-7.simg");
    }

    @Test
    public void testSingulartiyHTTPSSif() {
        this.testSingulartiy(
                "salmonella_ice",
                "salmonella_ice.sif",
                "https://workflow.isi.edu/scratch/rynge/ffh-workflow_latest.sif");
    }

    @Test
    public void testSingulartiyHTTPSPostImg() {
        this.testSingulartiy(
                "test", "test.simg", "http://pegasus.isi.edu/container.php?rid=/centos-7.simg");
    }

    @Test
    public void testSingulartiyHTTPTar() {
        this.testSingulartiy(
                "test", "test.tar", "http:///pegasus.isi.edu/images/singularity/centos-7.tar");
    }

    @Test
    public void testSingulartiyHTTPTarGZ() {
        this.testSingulartiy(
                "test",
                "test.tar.gz",
                "http:///pegasus.isi.edu/images/singularity/centos-7.tar.gz");
    }

    @Test
    public void testSingulartiyHTTPTarBZ() {
        this.testSingulartiy(
                "test",
                "test.tar.bz2",
                "http:///pegasus.isi.edu/images/singularity/centos-7.tar.bz2");
    }

    @Test
    public void testSingulartiyHTTPCPIO() {
        this.testSingulartiy(
                "test", "test.cpio", "http:///pegasus.isi.edu/images/singularity/centos-7.cpio");
    }

    @Test
    public void testSingulartiyHTTPTarCPIO() {
        this.testSingulartiy(
                "test",
                "test.cpio.gz",
                "http:///pegasus.isi.edu/images/singularity/centos-7.cpio.gz");
    }

    public void testSingulartiy(String name, String expectedLFN, String url) {
        Container c = new Container(name);
        c.setType(Container.TYPE.singularity);
        String lfn = c.computeLFN(new PegasusURL(url));
        assertEquals(expectedLFN, lfn);
    }

    @Test
    public void testContainerDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: centos-pegasus\n"
                        + "type: docker\n"
                        + "image: docker:///rynge/montage:latest\n"
                        + "mounts: \n"
                        + "  - /Volumes/Work/lfs1:/shared-data/:ro\n"
                        + "  - /Volumes/Work/lfs12:/shared-data1/:ro\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: /opt/java/1.6";

        Container c = mapper.readValue(test, Container.class);
        assertNotNull(c);
        assertEquals(Container.TYPE.docker, c.getType());
        assertEquals("docker:///rynge/montage:latest", c.getImageURL().getURL());

        assertEquals(2, c.getMountPoints().size());
        assertThat(
                c.getMountPoints(), hasItem(new MountPoint("/Volumes/Work/lfs1:/shared-data/:ro")));
        assertThat(
                c.getMountPoints(), hasItem(new MountPoint("/Volumes/Work/lfs1:/shared-data/:ro")));

        List<Profile> profiles = c.getProfiles("env");
        assertThat(profiles, hasItem(new Profile("env", "JAVA_HOME", "/opt/java/1.6")));
    }
}
