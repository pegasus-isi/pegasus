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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAX2CDAG;
import edu.isi.pegasus.planner.parser.dax.DAXParser;
import edu.isi.pegasus.planner.parser.dax.ExampleDAXCallback;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TestDAXParserTest {

    @Test
    public void loadDAXParserWithExampleCallbackForSampleDax() throws Exception {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart("test.dax.parser", "load", "0");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        String dax =
                Path.of(
                                "test",
                                "junit",
                                "edu",
                                "isi",
                                "pegasus",
                                "planner",
                                "parser",
                                "dax",
                                "input",
                                "blackdiamond.dax")
                        .toAbsolutePath()
                        .toString();

        DAXParser parser = DAXParserFactory.loadDAXParser(bag, "ExampleDAXCallback", dax);

        assertThat(parser, notNullValue());
        Callback callback = parser.getDAXCallback();
        assertThat(callback, instanceOf(ExampleDAXCallback.class));
        logger.logEventCompletion();
    }

    @Test
    public void getDAXMetadataReturnsHeaderFieldsForSampleDax() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart("test.dax.parser", "metadata", "0");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

        String dax =
                Path.of(
                                "test",
                                "junit",
                                "edu",
                                "isi",
                                "pegasus",
                                "planner",
                                "parser",
                                "dax",
                                "input",
                                "blackdiamond.dax")
                        .toAbsolutePath()
                        .toString();

        Map metadata = DAXParserFactory.getDAXMetadata(bag, dax);

        assertThat(metadata.get("name"), is("diamond"));
        assertThat(metadata.get("version"), is("3.6"));
        assertThat(metadata.get("count"), is("1"));
        assertThat(metadata.get("index"), is("0"));
        logger.logEventCompletion();
    }

    @Test
    public void defaultCallbackClassLoadsPlannerCallback() {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        Callback callback =
                DAXParserFactory.loadDAXParserCallback(
                        bag, "dummy.dax", DAXParserFactory.DEFAULT_CALLBACK_CLASS);

        assertThat(callback, notNullValue());
        assertThat(callback, instanceOf(DAX2CDAG.class));
    }
}
