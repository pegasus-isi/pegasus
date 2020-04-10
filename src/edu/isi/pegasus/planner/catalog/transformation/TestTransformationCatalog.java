/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.catalog.transformation;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.List;
import java.util.Properties;

/**
 * A Test program that shows how to load a Replica Catalog, and query for entries sites. The
 * configuration is picked from the Properties. The following properties need to be set
 *
 * <pre>
 *      pegasus.catalog.transformation       File|Database
 *      pegasus.catalog.transformation.file  path to the File Based Transformation Catalog if File is being used.
 *  </pre>
 *
 * To use the Database Transformation catalog the database connection parameters can be specified by
 * specifying the following properties
 *
 * <pre>
 *   pegasus.catalog.transformation.db.url
 *   pegasus.catalog.transformation.db.user
 *   pegasus.catalog.transformation.db.password
 * </pre>
 *
 * The sql schema’s for this catalog can be found at $PEGASUS_HOME/sql directory.
 *
 * <p>The Pegasus Properties can be picked from property files at various locations. The priorities
 * are explained below.
 *
 * <pre>
 *   - The default path for the properties file is $PEGASUS_HOME/etc/properties.
 *   - A properties file if found at ${user.home}/.pegasusrc has higher property.
 *   - Finally a user can specify the path to the properties file by specifying
 *     the JVM  property pegasus.user.properties . This has the higher priority.
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TestTransformationCatalog {

    /** The main program. */
    public static void main(String[] args) {
        TransformationCatalog catalog = null;
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        // setup the logger for the default streams.
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart(
                "event.pegasus.catalog.transformation.test",
                "planner.version",
                Version.instance().toString());

        // set debug level to maximum
        // set if something is going wrong
        // logger.setLevel( LogManager.DEBUG_MESSAGE_LEVEL );

        /* print out all the relevant site catalog properties that were specified*/
        Properties replicaProperties =
                properties.matchingSubset("pegasus.catalog.transformation", true);
        System.out.println("Transformation Catalog Properties specified are " + replicaProperties);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

        /* load the catalog using the factory */
        try {
            catalog = TransformationFactory.loadInstance();
        } catch (TransformationFactoryException e) {
            System.out.println(e.convertException());
            System.exit(2);
        }

        /* lets insert an entry into TransformationCatalog */
        try {
            /* the logical transformation is identified by a tuple
             * consisting of namespace , name and version
             * The namespace and version can be null.
             */
            String namespace = "pegasus";
            String lfn = "preprocess";
            String version = null;
            String pfn = "/usr/pegasus/bin/keg";
            String handle = "isi"; // the site handle of the site where the data is

            TransformationCatalogEntry tce =
                    new TransformationCatalogEntry(namespace, lfn, version);
            tce.setPhysicalTransformation(pfn);
            tce.setResourceId(handle);
            tce.setType(TCType.INSTALLED); // executable is installed
            tce.setVDSSysInfo(new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null));
            // add an environment profile with the entry
            tce.addProfile(new Profile(Profile.ENV, "PEGASUS_HOME", "/usr/pegasus/bin"));

            /* insert the entry into transformation catalog */
            boolean added = (catalog.insert(tce) == 1) ? true : false;
            System.out.println("Entry added " + added);

            /* query for the entry we just entered */
            List<TransformationCatalogEntry> results =
                    catalog.lookup(namespace, lfn, version, handle, TCType.INSTALLED);
            if (results != null) {
                System.out.println("Results for LFN " + lfn);
                for (TransformationCatalogEntry entry : results) {
                    System.out.println(entry);
                }
            }

            /* remove the entry we added
             * deletes are only implemented for the database version
             */
            // boolean deleted = catalog.removeByLFN( namespace, lfn, version, handle,
            // TCType.INSTALLED );
            // System.out.println( "Entry deleted " + added );

            /* list all the entries remaining in the TC */
            System.out.println("\nListing all entries in the transformation catalog ");
            List<TransformationCatalogEntry> entries = catalog.getContents();
            for (TransformationCatalogEntry e : entries) {
                System.out.println(e);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /* close the connection */
            try {
                catalog.close();
            } catch (Exception e) {
            }
        }
    }
}
