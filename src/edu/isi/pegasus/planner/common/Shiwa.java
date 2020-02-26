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
package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import org.shiwa.desktop.data.description.handler.TransferDependency;
import org.shiwa.desktop.data.description.handler.TransferPort;
import org.shiwa.desktop.data.description.handler.TransferSignature;
import org.shiwa.desktop.data.description.handler.TransferSignature.ValueType;
import org.shiwa.desktop.data.transfer.BundleReader;
import org.shiwa.desktop.data.util.exception.SHIWADesktopIOException;

/**
 * A convenience class that allows us to read Shiwa Bundles and setup Pegasus accordingly
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Shiwa {

    public static final String PROPERTIES_LFN = "pegasus-properties";

    public static final String SITE_CATALOG_LFN = "site-catalog";

    public static final String TRANSFORMATION_CATALOG_LFN = "transformation-catalog";

    /** The name of the source key for Replica Catalog Implementer t */
    public static final String REPLICA_CATALOG_KEY = "file";

    /**
     * The name of the Replica Catalog Implementer to which port and output locations are written
     * out from the bundle
     */
    public static final String REPLICA_CATALOG_IMPLEMENTER = "SimpleFile";

    /** The handle to pegasus logger */
    private LogManager mLogger;

    /**
     * The constructor
     *
     * @param logger
     */
    public Shiwa(LogManager logger) {
        mLogger = logger;
        /*
        Logger root = Logger.getRootLogger();
        if( root != null ){
            root.setLevel( Level.DEBUG );
        }*/
    }

    /**
     * Untars a shiwa bundle and updates the properties and options to refer to the contents of the
     * bundle
     *
     * @param shiwaBundle the SHIWA Bundle
     * @param properties the properties object that is updated
     * @param options the planner options that are updated
     */
    public void readBundle(
            String shiwaBundle, PegasusProperties properties, PlannerOptions options) {

        String dax = null;

        File f = new File(shiwaBundle);
        if (!f.exists()) {
            throw new RuntimeException("The shiwa bundle file does not exist " + shiwaBundle);
        }

        // we can only untar contents of bundle to the base submit directory
        File directory = new File(options.getSubmitDirectory());
        try {
            directory = File.createTempFile("shiwa-", "-bundle", directory);

            // potential for race condition
            directory.delete();
            directory.mkdirs();
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to create temp directory in directory " + directory);
        }

        mLogger.log(
                "The shiwa bundle will be unzipped to directory " + directory.getAbsolutePath(),
                LogManager.INFO_MESSAGE_LEVEL);

        BundleReader reader = null;
        Properties bundleProperties =
                new Properties(); // stores the properties in the properties file in the bundle
        Properties catalogProperties = new Properties();
        try {
            reader = new BundleReader(f, directory);
            dax = reader.getDefinitonFile().getAbsolutePath();

            mLogger.log("DAX File in the bundle resides at " + dax, LogManager.DEBUG_MESSAGE_LEVEL);

            TransferSignature signature = reader.getTransferSignature();

            // check to see if it is a DAX Bundle
            String language = signature.getLanguage();
            if (language == null || !signature.getLanguage().equalsIgnoreCase("DAX")) {
                throw new RuntimeException(
                        "Unsupported language type in the SHIWA bundle " + language);
            }

            // iterate through all the dependencies
            for (TransferDependency dependency : signature.getDependencies()) {
                String name = dependency.getName();
                ValueType type = dependency.getValueType();

                String file = dependency.getValue();
                if (name.equals(Shiwa.PROPERTIES_LFN)) {
                    // we have the properties object
                    if (type == ValueType.BUNDLED_FILE) {
                        mLogger.log(
                                "Path to Properties File in Bundle " + file,
                                LogManager.INFO_MESSAGE_LEVEL);
                    } else {
                        // log a warning
                        mLogger.log(
                                "Properties File is not Bundled. A URI was specified " + file,
                                LogManager.WARNING_MESSAGE_LEVEL);
                    }
                    Properties p = new Properties();
                    p.load(new FileReader(file));
                    bundleProperties.putAll(p);
                } else if (name.equals(Shiwa.SITE_CATALOG_LFN)) {
                    // we have the site catalog
                    if (type == ValueType.BUNDLED_FILE) {
                        mLogger.log(
                                "Path to Site Catalog File in Bundle " + file,
                                LogManager.INFO_MESSAGE_LEVEL);
                    } else {
                        // log a warning
                        mLogger.log(
                                "Site Catalog File is not Bundled. A URI was specified " + file,
                                LogManager.WARNING_MESSAGE_LEVEL);
                    }
                    catalogProperties.put(
                            PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY, file);
                } else if (name.equals(Shiwa.TRANSFORMATION_CATALOG_LFN)) {
                    // we have the transformation catalog
                    // we have the site catalog
                    if (type == ValueType.BUNDLED_FILE) {
                        mLogger.log(
                                "Path to Transformation Catalog File in Bundle " + file,
                                LogManager.INFO_MESSAGE_LEVEL);
                    } else {
                        // log a warning
                        mLogger.log(
                                "Transformation Catalog File is not Bundled. A URI was specified "
                                        + file,
                                LogManager.WARNING_MESSAGE_LEVEL);
                    }
                    catalogProperties.put(
                            PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY, file);
                }
            }

            // put in the catalog properties into the bundle properties
            bundleProperties.putAll(catalogProperties);

            // if dax is specified in the Shiwa Bundle
            // update the options accordingly
            if (dax != null) {
                // sanity check
                String original = options.getDAX();
                if (original != null) {
                    mLogger.log(
                            "A DAX file was specified at command line. "
                                    + original
                                    + " Will be overriden by the one in the bundle "
                                    + dax,
                            LogManager.WARNING_MESSAGE_LEVEL);
                }
                options.setDAX(dax);

                // directory is the directory where shiwa bundle has been
                // untarred
                // directory = new File( dax ).getParentFile();
            }

            String rc = generateReplicaCatalogFile(signature, directory);

            mLogger.log(
                    "Generated the replica catalog file from the bundle " + rc,
                    LogManager.INFO_MESSAGE_LEVEL);

            // put in the property for the generated replica catalog
            bundleProperties.put(
                    PegasusProperties.PEGASUS_REPLICA_CATALOG_PROPERTY,
                    Shiwa.REPLICA_CATALOG_IMPLEMENTER);
            bundleProperties.put(
                    PegasusProperties.PEGASUS_REPLICA_CATALOG_PROPERTY
                            + "."
                            + Shiwa.REPLICA_CATALOG_KEY,
                    rc);

            // the properties specified in the bundle are put into Pegasus Properties
            for (Iterator it = bundleProperties.keySet().iterator(); it.hasNext(); ) {
                String key = (String) it.next();
                properties.setProperty(key, bundleProperties.getProperty(key));
            }
        } catch (SHIWADesktopIOException ex) {
            throw new RuntimeException("Unable to create Shiwa Bundle Object ", ex);
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Problem retrieving the DAX file from the bundle " + shiwaBundle, ioe);
        }
    }

    /**
     * Generates a Replica Catalog file based on the input and output ports in the Shiwa Bundle
     *
     * @param signature the transfer signature for the SHIWA Bundle
     * @param directory the directory in which the replica catalog should be generated
     * @return the replica catalog file
     */
    public String generateReplicaCatalogFile(TransferSignature signature, File directory) {
        ReplicaCatalog rc = null;
        mLogger.log(
                "Generating the Replica Catalog From the Bundle ", LogManager.DEBUG_MESSAGE_LEVEL);

        // we will write out the Replica Catalog based on port
        // and output ports in the bundle
        if (directory == null) {
            try {
                directory = File.createTempFile("pegasus", "config");
                directory.mkdirs();
            } catch (IOException ioe) {
                throw new RuntimeException("Unable to create a directory " + directory);
            }
        }

        String rcFile = directory.getAbsolutePath() + File.separator + "rc.data";

        Properties props = new Properties();

        // set the appropriate property to designate path to file
        props.setProperty(Shiwa.REPLICA_CATALOG_KEY, rcFile);

        try {
            rc = ReplicaFactory.loadInstance(REPLICA_CATALOG_IMPLEMENTER, props);
        } catch (Exception e) {
            throw new RuntimeException("Unable to replica catalog  " + rcFile, e);
        }

        // go through the port and output ports
        this.addPortsToReplicaCatalog(rc, signature.getInputs(), true);
        this.addPortsToReplicaCatalog(rc, signature.getOutputs(), false);

        // close the rc file
        rc.close();

        return rcFile;
    }

    /**
     * Adds the ports specified in the bundle to the replica catalog file.
     *
     * @param rc handle to the replica catalog
     * @param ports the collection of ports
     * @param inputPort boolean indicating whether the ports passed are input ports
     */
    public void addPortsToReplicaCatalog(
            ReplicaCatalog rc, Collection<TransferPort> ports, boolean inputPort) {
        for (TransferPort port : ports) {
            String lfn = port.getName();
            String pfn = port.getValue();
            String pool = "undefined";

            if (port.getValueType() == ValueType.BUNDLED_FILE) {
                // prepend file url if required
                if (pfn.startsWith(File.separator)) {
                    pfn = "file://" + pfn;
                }
            }

            // update the pool to be local in case of file urls
            if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
                pool = "local";
            }

            // only insert if pfn is not null or empty
            if (pfn == null || pfn.isEmpty()) {
                mLogger.log(
                        "Not inserting entry for lfn retrieved from shiwa bundle " + lfn,
                        LogManager.TRACE_MESSAGE_LEVEL);
                // log a warning for a missing input port
                if (inputPort) {
                    mLogger.log(
                            "PFN not specified for input lfn retrieved from shiwa bundle " + lfn,
                            LogManager.WARNING_MESSAGE_LEVEL);
                }

            } else {
                rc.insert(lfn, pfn, pool);
            }
        }
    }
}
