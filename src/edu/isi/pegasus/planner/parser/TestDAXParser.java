/**
 * Copyright 2007-2008 University Of Southern California
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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * A Test Class to demonstrate use of DAXParser and illustrates how to use the Callbacks for the
 * parser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TestDAXParser {

    /**
     * The main program to TestDAXParser.
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("The class takes in one argument - the path to the DAX file");
            System.exit(1);
        }

        String daxFile = args[0];

        /* get handle to the Pegasus Properties File*/
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        /* instantiate the internal Pegasus Logger */
        // setup the logger for the default streams.
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart(
                "example.dax.parser", "planner.version", Version.instance().toString());
        logger.setLevel(5);

        /* pass the logger and properties to Pegasus Bag*/
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        /* instantiate the DAX Parser and start parsing */
        try {
            //           DaxParser parser = new DaxParser( daxFile, bag, mycallback );
            // DAXParser3 parser3 = new DAXParser3( daxFile, bag, mycallback );
            Parser daxParser =
                    (Parser) DAXParserFactory.loadDAXParser(bag, "ExampleDAXCallback", daxFile);
            daxParser.startParser(daxFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
