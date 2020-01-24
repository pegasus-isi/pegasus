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

package edu.isi.pegasus.planner.catalog.replica;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A Test program that shows how to load a Replica Catalog, and query for entries sites. The
 * configuration is picked from the Properties. The following properties need to be set
 *
 * <pre>
 *      pegasus.catalog.replica       SimpleFile|JDCBCRC|RLS|LRC
 *      pegasus.catalog.replica.file  path to the Simple File Replica catalog if SimpleFile is being used.
 *      pegasus.catalog.replica.url   The RLS url  if replica catalog being used is RLS or LRC
 *  </pre>
 *
 * To use the JDBCRC the database connection parameters can be specified by specifying the following
 * properties
 *
 * <pre>
 *   pegasus.catalog.replica.db.url
 *   pegasus.catalog.replica.db.user
 *   pegasus.catalog.replica.db.password
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
public class TestReplicaCatalog {

    /** The main program. */
    public static void main(String[] args) {
        ReplicaCatalog catalog = null;
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        // setup the logger for the default streams.
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart(
                "event.pegasus.catalog.replica.test",
                "planner.version",
                Version.instance().toString());

        // set debug level to maximum
        // set if something is going wrong
        // logger.setLevel( LogManager.DEBUG_MESSAGE_LEVEL );

        /* print out all the relevant site catalog properties that were specified*/
        Properties replicaProperties = properties.matchingSubset("pegasus.catalog.replica", true);
        System.out.println("Replica Catalog Properties specified are " + replicaProperties);

        /* load the catalog using the factory */
        try {
            catalog = ReplicaFactory.loadInstance(PegasusProperties.nonSingletonInstance());
        } catch (Exception e) {
            System.out.println(e);
            System.exit(2);
        }

        /* lets insert an entry into ReplicaCatalog */
        try {
            String lfn = "test.replica";
            String pfn = "gsiftp://test.isi.edu/examples/test.replica";
            String handle = "isi"; // the site handle of the site where the data is
            ReplicaCatalogEntry rce = new ReplicaCatalogEntry(pfn, handle);

            /* insert the RCE into Replica Catalog
             * multiple RCE for the same LFN can be inserted */
            catalog.insert(lfn, rce);

            /* insert another */
            catalog.insert(lfn, "gsiftp://replica.isi.edu/examples/test.replica", "local");

            /* query for the entry we just entered */
            Collection<ReplicaCatalogEntry> results = catalog.lookup(lfn);
            System.out.println("Results for LFN " + lfn);
            for (ReplicaCatalogEntry entry : results) {
                System.out.println(entry);
            }

            /* remove the first entry */
            catalog.delete(lfn, rce);

            /* can remove all the entries associated with a lfn */
            // catalog.remove( lfn );

            /* list all the entries remaining in the Replica Catalog */
            Set<String> lfns = catalog.list();
            System.out.println("LFN's in replica catalog " + lfns);

            /* we can do a bulk lookup of lfns */
            Map<String, Collection<ReplicaCatalogEntry>> entries = catalog.lookup(lfns);
            for (Iterator<Map.Entry<String, Collection<ReplicaCatalogEntry>>> it =
                            entries.entrySet().iterator();
                    it.hasNext(); ) {
                Map.Entry<String, Collection<ReplicaCatalogEntry>> entry = it.next();
                String logicalFilename = entry.getKey();
                results = catalog.lookup(logicalFilename);

                System.out.println("Results for LFN " + logicalFilename);
                for (ReplicaCatalogEntry result : results) {
                    System.out.println(result);
                }
            }

        } catch (ReplicaCatalogException e) {
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
