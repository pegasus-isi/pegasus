package edu.isi.pegasus.common.util;

import java.util.Properties;

import org.junit.Test;
import org.junit.Before;
import org.junit.Assert;

import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;

public class VersionTest {
    private Properties props = new Properties();
    private Version ver = new Version();

    public VersionTest() {}

    @Before
    public void loadProperties() throws Exception {
        props.load(Version.class.getClassLoader().getResourceAsStream("pegasus.build.properties"));
    }

    @Test
    public void testVersion() {
        String version = ver.getVersion();
        Assert.assertTrue(version != null);
        Assert.assertTrue(version.matches("\\d+[.]\\d+[.]\\d+([a-zA-Z0-9]+)?"));
    }

    @Test
    public void testMajorMinorPatch() {
        String major = ver.getMajor();
        Assert.assertTrue(major != null);
        Assert.assertTrue(major.matches("\\d+"));

        String minor = ver.getMinor();
        Assert.assertTrue(minor != null);
        Assert.assertTrue(minor.matches("\\d+"));

        String patch = ver.getPatch();
        Assert.assertTrue(patch != null);
        Assert.assertTrue(patch.matches("\\d+([a-zA-Z0-9]+)?"));

        String version = ver.getVersion();
        Assert.assertEquals(version, major + "." + minor + "." + patch);
    }

    @Test
    public void testTimestamp() {
        String ts = ver.getTimestamp();
        Assert.assertTrue(ts != null);
        Assert.assertTrue(ts.matches("\\d{14}Z"));
    }

    @Test
    public void testPlatform() {
        String plat = ver.getPlatform();
        Assert.assertTrue(plat != null);
    }
    
    @Test
    public void testArchitceture() {
        SysInfo.Architecture arch = ver.getArchitecture();
        Assert.assertTrue(arch != null);
    }
    
    @Test
    public void testOS() {
        SysInfo.OS  os = ver.getOS();
        Assert.assertTrue(os != null);
    }
    
    @Test
    public void testOSReleaese() {
        SysInfo.OS_RELEASE release = ver.getOSRelease();
        Assert.assertTrue(release != null);
    }
}

