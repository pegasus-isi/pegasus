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
package edu.isi.pegasus.planner.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import gnu.getopt.LongOpt;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class VDS2PegasusPropertiesTest {

    @Test
    public void testGenerateValidOptions() {
        VDS2PegasusProperties converter = new VDS2PegasusProperties();

        LongOpt[] options = converter.generateValidOptions();

        assertThat(options.length, is(4));
        assertThat(options[0].getName(), is("input"));
        assertThat(options[0].getHasArg(), is(LongOpt.REQUIRED_ARGUMENT));
        assertThat(options[0].getVal(), is((int) 'i'));
        assertThat(options[1].getName(), is("output"));
        assertThat(options[2].getName(), is("help"));
        assertThat(options[2].getHasArg(), is(LongOpt.NO_ARGUMENT));
        assertThat(options[3].getName(), is("conf"));
        assertThat(options[3].getVal(), is((int) 'c'));
    }

    @Test
    public void testMatchForStarPropertiesMatchesKnownPatternsAndUnknownReturnsNull() {
        VDS2PegasusProperties converter = new VDS2PegasusProperties();

        assertThat(
                converter.matchForStarProperties("vds.replica.siteA.prefer.stagein.sites"),
                is("pegasus.selector.replica.siteA.prefer.stagein.sites"));
        assertThat(
                converter.matchForStarProperties("vds.db.tc.driver.mysql"),
                is("pegasus.catalog.transformation.db.mysql"));
        assertThat(converter.matchForStarProperties("vds.unmapped.property"), nullValue());
    }

    @Test
    public void testMatchingSubsetKeepsOrStripsPrefixAsConfigured() {
        VDS2PegasusProperties converter = new VDS2PegasusProperties();
        Properties properties = new Properties();
        properties.setProperty("vds.alpha", "one");
        properties.setProperty("vds.beta", "two");
        properties.setProperty("vds", "root");
        properties.setProperty("other", "ignored");

        Properties kept = converter.matchingSubset(properties, "vds", true);
        assertThat(kept.getProperty("vds.alpha"), is("one"));
        assertThat(kept.getProperty("vds.beta"), is("two"));
        assertThat(kept.getProperty("vds"), is("root"));

        Properties stripped = converter.matchingSubset(properties, "vds", false);
        assertThat(stripped.getProperty("alpha"), is("one"));
        assertThat(stripped.getProperty("beta"), is("two"));
        assertThat(stripped.getProperty("vds"), nullValue());
        assertThat(stripped.getProperty("other"), nullValue());
    }

    @Test
    public void testMatchingSubsetReturnsEmptyForBlankPrefix() {
        VDS2PegasusProperties converter = new VDS2PegasusProperties();
        Properties properties = new Properties();
        properties.setProperty("vds.alpha", "one");

        assertThat(converter.matchingSubset(properties, "", true).isEmpty(), is(true));
        assertThat(converter.matchingSubset(properties, null, false).isEmpty(), is(true));
    }

    @Test
    public void testSanityCheckCreatesDirectoriesAndRejectsFiles() throws IOException {
        File tempRoot = Files.createTempDirectory("vds2pegasus-properties-test").toFile();
        File missingDir = new File(tempRoot, "new-output-dir");

        VDS2PegasusProperties.sanityCheck(missingDir);
        assertThat(missingDir.isDirectory(), is(true));

        File plainFile = new File(tempRoot, "plain-file.txt");
        Files.write(plainFile.toPath(), "content".getBytes());

        IOException exception =
                assertThrows(
                        IOException.class,
                        () -> VDS2PegasusProperties.sanityCheck(plainFile),
                        "sanityCheck should reject existing plain files");

        assertThat(exception.getMessage(), containsString("is not a directory"));
    }
}
