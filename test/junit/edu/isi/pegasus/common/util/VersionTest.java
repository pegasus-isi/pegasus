package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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
        assertThat(version, is(notNullValue()));
        assertThat(version.matches("\\d+[.]\\d+[.]\\d+(-(dev|alpha|beta|rc).\\d+)?"), is(true));
    }

    @Test
    public void testMajorMinorPatch() {
        String major = ver.getMajor();
        assertThat(major, is(notNullValue()));
        assertThat(major.matches("\\d+"), is(true));

        String minor = ver.getMinor();
        assertThat(minor, is(notNullValue()));
        assertThat(minor.matches("\\d+"), is(true));

        String patch = ver.getPatch();
        assertThat(patch, is(notNullValue()));
        assertThat(patch.matches("\\d+(-(dev|alpha|beta|rc).\\d+)?"), is(true));

        String version = ver.getVersion();
        assertThat(version, is(major + "." + minor + "." + patch));
    }

    @Test
    public void testTimestamp() {
        String ts = ver.getTimestamp();
        assertThat(ts, is(notNullValue()));
        assertThat(ts.matches("\\d{14}Z"), is(true));
    }

    @Test
    public void testPlatform() {
        String plat = ver.getPlatform();
        assertThat(plat, is(notNullValue()));
    }

    @Test
    public void testArchitceture() {
        SysInfo.Architecture arch = ver.getArchitecture();
        assertThat(arch, is(notNullValue()));
    }

    @Test
    public void testOS() {
        SysInfo.OS os = ver.getOS();
        assertThat(os, is(notNullValue()));
    }

    @Test
    public void testOSReleaese() {
        SysInfo.OS_RELEASE release = ver.getOSRelease();
        assertThat(release, is(notNullValue()));
    }
}
