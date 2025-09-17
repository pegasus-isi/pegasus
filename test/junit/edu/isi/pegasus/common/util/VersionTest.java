package edu.isi.pegasus.common.util;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VersionTest {
    private Properties props = new Properties();
    private Version ver = new Version();

    public VersionTest() {}

    @BeforeEach
    public void loadProperties() throws Exception {
        props.load(Version.class.getClassLoader().getResourceAsStream("pegasus.build.properties"));
    }

    @Test
    public void testVersion() {
        String version = ver.getVersion();
        assertTrue(version != null);
        assertTrue(version.matches("\\d+[.]\\d+[.]\\d+(-(dev|alpha|beta|rc).\\d+)?"));
    }

    @Test
    public void testMajorMinorPatch() {
        String major = ver.getMajor();
        assertTrue(major != null);
        assertTrue(major.matches("\\d+"));

        String minor = ver.getMinor();
        assertTrue(minor != null);
        assertTrue(minor.matches("\\d+"));

        String patch = ver.getPatch();
        assertTrue(patch != null);
        assertTrue(patch.matches("\\d+(-(dev|alpha|beta|rc).\\d+)?"));

        String version = ver.getVersion();
        assertEquals(version, major + "." + minor + "." + patch);
    }

    @Test
    public void testTimestamp() {
        String ts = ver.getTimestamp();
        assertTrue(ts != null);
        assertTrue(ts.matches("\\d{14}Z"));
    }

    @Test
    public void testPlatform() {
        String plat = ver.getPlatform();
        assertTrue(plat != null);
    }

    @Test
    public void testArchitceture() {
        SysInfo.Architecture arch = ver.getArchitecture();
        assertTrue(arch != null);
    }

    @Test
    public void testOS() {
        SysInfo.OS os = ver.getOS();
        assertTrue(os != null);
    }

    @Test
    public void testOSReleaese() {
        SysInfo.OS_RELEASE release = ver.getOSRelease();
        assertTrue(release != null);
    }
}
