package edu.isi.pegasus.common.util;

import java.util.Properties;

import org.junit.Test;
import org.junit.Before;
import org.junit.Assert;

import edu.isi.pegasus.common.util.Version;

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
        Assert.assertTrue(version.matches("\\d+[.]\\d+[.]\\d+(.*)?"));
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
        Assert.assertTrue(patch.matches("\\d+(.*)?"));

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
}

