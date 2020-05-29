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
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.ShellCommand;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.DirectoryLayout;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SharedDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleFactory;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A utility class that returns JAVA Properties that need to be set based on a configuration value
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusConfiguration {

    /** The property key for pegasus configuration. */
    public static final String PEGASUS_CONFIGURATION_PROPERTY_KEY = "pegasus.data.configuration";

    /** The value for the S3 configuration. */
    public static final String DEPRECATED_S3_CONFIGURATION_VALUE = "S3";

    /** The value for the non shared filesystem configuration. */
    public static final String SHARED_FS_CONFIGURATION_VALUE = "sharedfs";

    /** The value for the condor configuration. */
    public static final String CONDOR_CONFIGURATION_VALUE = "condorio";

    /** The default data configuration value */
    public static String DEFAULT_DATA_CONFIGURATION_VALUE = CONDOR_CONFIGURATION_VALUE;

    /** The value for the non shared filesystem configuration. */
    public static final String NON_SHARED_FS_CONFIGURATION_VALUE = "nonsharedfs";

    /** The value for the condor configuration. */
    public static final String DEPRECATED_CONDOR_CONFIGURATION_VALUE = "Condor";

    /** The logger to use. */
    private LogManager mLogger;

    /** The Version to use */
    private Version mVersion;

    /**
     * Overloaded Constructor
     *
     * @param logger the logger to use.
     */
    public PegasusConfiguration(LogManager logger) {
        mLogger = logger;
        mVersion = new Version();
    }

    /**
     * Loads configuration specific properties into PegasusProperties, and adjusts planner options
     * accordingly.
     *
     * @param properties the Pegasus Properties
     * @param options the PlannerOptions .
     */
    public void loadConfigurationPropertiesAndOptions(
            PegasusProperties properties, PlannerOptions options) {

        this.loadConfigurationProperties(properties);

        // PM-1190 if integrity checking is turned on, turn on the stat of
        // files also
        if (properties.doIntegrityChecking()) {
            // this.checkAndSetProperty(properties,
            // PegasusProperties.PEGASUS_KICKSTART_STAT_PROPERTY, "true" );
        }
    }

    /**
     * Returns the staging site to be used for a job. The determination is made on the basis of the
     * following - data configuration value for job - from planner command line options - If a
     * staging site is not determined from the options it is set to be the execution site for the
     * job
     *
     * @param job the job for which to determine the staging site
     * @return the staging site
     */
    public String determineStagingSite(Job job, PlannerOptions options) {
        // check to see if job has data.configuration set
        if (!job.vdsNS.containsKey(Pegasus.DATA_CONFIGURATION_KEY)) {
            throw new RuntimeException(
                    "Internal Planner Error: Data Configuration should have been set for job "
                            + job.getID());
        }

        String conf = job.getDataConfiguration();
        // shortcut for condorio
        if (conf.equalsIgnoreCase(PegasusConfiguration.CONDOR_CONFIGURATION_VALUE)) {
            // sanity check against the command line option
            // we are leaving the data configuration to be per site
            // by this check
            String stagingSite = options.getStagingSite(job.getSiteHandle());
            if (stagingSite == null) {
                stagingSite = "local";
            } else if (!(stagingSite.equalsIgnoreCase("local"))) {
                StringBuffer sb = new StringBuffer();

                sb.append("Mismatch in the between execution site ")
                        .append(job.getSiteHandle())
                        .append(" and staging site ")
                        .append(stagingSite)
                        .append(" for job ")
                        .append(job.getID())
                        .append(" . For Condor IO staging site should be set to local .");

                throw new RuntimeException(sb.toString());
            }

            return stagingSite;
        }

        String ss = options.getStagingSite(job.getSiteHandle());
        ss = (ss == null) ? job.getSiteHandle() : ss;
        // check for sharedfs
        if (conf.equalsIgnoreCase(PegasusConfiguration.SHARED_FS_CONFIGURATION_VALUE)
                && !ss.equalsIgnoreCase(job.getSiteHandle())) {
            StringBuffer sb = new StringBuffer();

            sb.append("Mismatch in the between execution site ")
                    .append(job.getSiteHandle())
                    .append(" and staging site ")
                    .append(ss)
                    .append(" for job ")
                    .append(job.getID())
                    .append(" . For sharedfs they should be the same");

            throw new RuntimeException(sb.toString());
        }

        return ss;
    }

    /**
     * Updates Site Store and options based on the planner options set by the user on the command
     * line
     *
     * @param store the outputSite store
     * @param options the planner options.
     */
    public void updateSiteStoreAndOptions(SiteStore store, PlannerOptions options) {
        
        File pegasusBinDir = FindExecutable.findExec("pegasus-version").getParentFile();
        String pegasusHome = pegasusBinDir.getParent();

        // check for local site or create a default entry
        if (!store.list().contains("local")) {
            store.addEntry(constructDefaultLocalSiteEntry(options, pegasusHome));
            mLogger.log(
                    "Constructed default site catalog entry for local site "
                            + store.lookup("local"),
                    LogManager.CONFIG_MESSAGE_LEVEL);
        }
        // PM-1516 check for condorpool site or create a default entry
        if (!store.list().contains("condorpool")) {
            store.addEntry(constructDefaultCondorPoolSiteEntry(options, pegasusHome));
            mLogger.log(
                    "Constructed default site catalog entry for condorpool site "
                            + store.lookup("condorpool"),
                    LogManager.CONFIG_MESSAGE_LEVEL);
        }

        for(String outputSite: options.getOutputSites()){
            if (outputSite != null) {
                if (!store.list().contains(outputSite)) {
                    StringBuffer error = new StringBuffer();
                    error.append("The output site [")
                            .append(outputSite)
                            .append("] not loaded from the site catalog.");
                    throw new RuntimeException(error.toString());
                }
            }
        }

        // check if a user specified an output directory
        String directory = options.getOutputDirectory();
        String externalDirectory =
                options.getOutputDirectory(); // the external view of the directory if relative
        if (directory != null) {
            outputSite =
                    (outputSite == null)
                            ? "local"
                            : // user did not specify an output site, default to local
                            outputSite; // stick with what user specified

            options.addOutputSite(outputSite);

            SiteCatalogEntry entry = store.lookup(outputSite);
            if (entry == null) {
                throw new RuntimeException(
                        "No entry found in site catalog for output site: " + outputSite);
            }

            // we first check for local directory
            DirectoryLayout storageDirectory = entry.getDirectory(Directory.TYPE.local_storage);
            if (storageDirectory == null || storageDirectory.isEmpty()) {
                // default to shared directory
                storageDirectory = entry.getDirectory(Directory.TYPE.shared_storage);
            }
            if (storageDirectory == null || storageDirectory.isEmpty()) {
                if (outputSite.equals("local")) {
                    // PM-1081 create a default storage directory for local site
                    // with a stub file server. follow up code will update
                    // it with the output-dir value
                    Directory dir = new Directory();
                    dir.setType(Directory.TYPE.local_storage);
                    dir.addFileServer(new FileServer("file", "file://", ""));
                    entry.addDirectory(dir);
                    storageDirectory = dir;
                } else {
                    // now throw an error
                    throw new RuntimeException(
                            "No storage directory specified for output site " + outputSite);
                }
            }

            boolean append =
                    (directory.startsWith(File.separator))
                            ? false
                            : // we use the path specified
                            true; // we need to append to path in site catalog on basis of site
            // handle

            // update the internal mount point and external URL's
            InternalMountPoint imp = storageDirectory.getInternalMountPoint();
            if (imp == null || imp.getMountPoint() == null) {
                // now throw an error
                throw new RuntimeException(
                        "No internal mount point specified  for HeadNode Storage Directory  for output site "
                                + outputSite);
            }

            if (append) {
                // we update the output directory on basis of what is specified
                // in the site catalog for the site. For local site, we resolve
                // the relative path from the command line/environment
                if (outputSite.equals("local")) {
                    directory = new File(directory).getAbsolutePath();
                    externalDirectory = directory;
                    // we have the directory figured out for local site
                    // that should be the one to be used for external file servers
                    append = false;
                } else {
                    directory = new File(imp.getMountPoint(), directory).getAbsolutePath();
                }
            }

            // update all storage file server paths to refer to the directory
            StringBuffer message = new StringBuffer();
            message.append("Updating internal storage file server paths for site ")
                    .append(outputSite)
                    .append(" to directory ")
                    .append(directory);
            mLogger.log(message.toString(), LogManager.CONFIG_MESSAGE_LEVEL);
            imp.setMountPoint(directory);

            for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
                for (Iterator<FileServer> it = storageDirectory.getFileServersIterator(op);
                        it.hasNext(); ) {
                    FileServer server = it.next();
                    if (append) {
                        // get the mount point and append
                        StringBuffer mountPoint = new StringBuffer();
                        mountPoint
                                .append(server.getMountPoint())
                                .append(File.separator)
                                .append(externalDirectory);
                        server.setMountPoint(mountPoint.toString());

                    } else {
                        server.setMountPoint(directory);
                    }
                }
            }

            // log the updated output site entry
            mLogger.log("Updated output site entry is " + entry, LogManager.DEBUG_MESSAGE_LEVEL);
        }

        // PM-960 lets do some post processing of the sites
        CondorStyleFactory factory = new CondorStyleFactory();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.getInstance());
        bag.add(PegasusBag.SITE_STORE, store);
        factory.initialize(bag);
        for (Iterator<SiteCatalogEntry> it = store.entryIterator(); it.hasNext(); ) {
            SiteCatalogEntry s = it.next();
            CondorStyle style = factory.loadInstance(s);
            mLogger.log(
                    "Style  detected for site " + s.getSiteHandle() + " is " + style.getClass(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            try {
                style.apply(s);
            } catch (CondorStyleException ex) {
                throw new RuntimeException("Unable to apply style to site " + s, ex);
            }
        }
    }

    /**
     * Loads configuration specific properties into PegasusProperties
     *
     * @param properties the Pegasus Properties.
     */
    private void loadConfigurationProperties(PegasusProperties properties) {
        String configuration = properties.getProperty(PEGASUS_CONFIGURATION_PROPERTY_KEY);

        Properties props = this.getConfigurationProperties(configuration);
        for (Iterator it = props.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            String value = props.getProperty(key);
            this.checkAndSetProperty(properties, key, value);
        }
    }

    /**
     * Returns Properties corresponding to a particular configuration.
     *
     * @param configuration the configuration value.
     * @return Properties
     */
    public Properties getConfigurationProperties(String configuration) {
        Properties p = new Properties();
        // sanity check
        if (configuration == null) {
            // default is the sharedfs
            configuration = SHARED_FS_CONFIGURATION_VALUE;
        }

        if (configuration.equalsIgnoreCase(DEPRECATED_S3_CONFIGURATION_VALUE)
                || configuration.equalsIgnoreCase(NON_SHARED_FS_CONFIGURATION_VALUE)) {

            // throw warning for deprecated value
            if (configuration.equalsIgnoreCase(DEPRECATED_S3_CONFIGURATION_VALUE)) {
                mLogger.log(
                        deprecatedValueMessage(
                                PEGASUS_CONFIGURATION_PROPERTY_KEY,
                                DEPRECATED_S3_CONFIGURATION_VALUE,
                                NON_SHARED_FS_CONFIGURATION_VALUE),
                        LogManager.WARNING_MESSAGE_LEVEL);
            }

            // we want the worker package to be staged, unless user sets it to false explicitly
            p.setProperty(PegasusProperties.PEGASUS_TRANSFER_WORKER_PACKAGE_PROPERTY, "true");
        } else if (configuration.equalsIgnoreCase(CONDOR_CONFIGURATION_VALUE)
                || configuration.equalsIgnoreCase(DEPRECATED_CONDOR_CONFIGURATION_VALUE)) {

            // throw warning for deprecated value
            if (configuration.equalsIgnoreCase(DEPRECATED_CONDOR_CONFIGURATION_VALUE)) {
                mLogger.log(
                        deprecatedValueMessage(
                                PEGASUS_CONFIGURATION_PROPERTY_KEY,
                                DEPRECATED_CONDOR_CONFIGURATION_VALUE,
                                CONDOR_CONFIGURATION_VALUE),
                        LogManager.WARNING_MESSAGE_LEVEL);
            }

            // we want the worker package to be staged, unless user sets it to false explicitly
            p.setProperty(PegasusProperties.PEGASUS_TRANSFER_WORKER_PACKAGE_PROPERTY, "true");

        } else if (configuration.equalsIgnoreCase(SHARED_FS_CONFIGURATION_VALUE)) {
            // PM-624
            // we should not explicitly set it to false. false is default value
            // in Pegasus Properties.
            // p.setProperty( "pegasus.execute.*.filesystem.local", "false" );
        } else {
            throw new RuntimeException(
                    "Invalid value "
                            + configuration
                            + " specified for property "
                            + PegasusConfiguration.PEGASUS_CONFIGURATION_PROPERTY_KEY);
        }

        return p;
    }

    /**
     * Returns a boolean indicating whether a job is setup for worker node execution or not
     *
     * @param job
     * @return
     */
    public boolean jobSetupForWorkerNodeExecution(Job job) {
        String configuration = job.getDataConfiguration();

        return (configuration == null)
                ? false
                : // DEFAULT is sharedfs case if nothing is specified
                (configuration.equalsIgnoreCase(CONDOR_CONFIGURATION_VALUE)
                        || configuration.equalsIgnoreCase(NON_SHARED_FS_CONFIGURATION_VALUE)
                        || configuration.equalsIgnoreCase(DEPRECATED_CONDOR_CONFIGURATION_VALUE));
    }

    /**
     * Returns a boolean indicating if job has to be executed using condorio
     *
     * @param job
     * @return boolean
     */
    public boolean jobSetupForCondorIO(Job job, PegasusProperties properties) {
        String configuration = job.getDataConfiguration();
        ;

        return (configuration == null)
                ? this.setupForCondorIO(properties)
                : configuration.equalsIgnoreCase(CONDOR_CONFIGURATION_VALUE)
                        || configuration.equalsIgnoreCase(DEPRECATED_CONDOR_CONFIGURATION_VALUE);
    }

    /**
     * Returns a boolean indicating if properties are setup for condor io
     *
     * @param properties
     * @return boolean
     */
    private boolean setupForCondorIO(PegasusProperties properties) {
        String configuration = properties.getProperty(PEGASUS_CONFIGURATION_PROPERTY_KEY);

        return (configuration == null)
                ? false
                : configuration.equalsIgnoreCase(CONDOR_CONFIGURATION_VALUE)
                        || configuration.equalsIgnoreCase(DEPRECATED_CONDOR_CONFIGURATION_VALUE);
    }

    /**
     * Checks for a property, if it does not exist then sets the property to the value passed
     *
     * @param key the property key
     * @param value the value to set to
     */
    protected void checkAndSetProperty(PegasusProperties properties, String key, String value) {
        String propValue = properties.getProperty(key);
        if (propValue == null) {
            // set the value
            properties.setProperty(key, value);
        } else {
            // log a warning
            StringBuffer sb = new StringBuffer();
            sb.append("Property Key ")
                    .append(key)
                    .append(" already set to ")
                    .append(propValue)
                    .append(". Will not be set to - ")
                    .append(value);
            mLogger.log(sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Returns the deperecated value message
     *
     * @param property the property
     * @param deprecatedValue the deprecated value
     * @param updatedValue the updated value
     * @return message
     */
    protected String deprecatedValueMessage(
            String property, String deprecatedValue, String updatedValue) {
        StringBuffer sb = new StringBuffer();
        sb.append(" The property ")
                .append(property)
                .append(" = ")
                .append(deprecatedValue)
                .append(" is deprecated. Replace with ")
                .append(property)
                .append(" = ")
                .append(updatedValue);

        return sb.toString();
    }

    /**
     * Constructs default SiteCatalogEntry for local site
     *
     * @param options
     * @param pegasusHome the pegasus home to be set
     * @return
     */
    private SiteCatalogEntry constructDefaultLocalSiteEntry(
            PlannerOptions options, String pegasusHome) {
        String submitDir = options.getSubmitDirectory();
        File scratch = new File(new File(submitDir).getParent(), "wf-scratch/LOCAL");
        File output = new File(new File(submitDir).getParent(), "wf-output");

        SiteCatalogEntry site = new SiteCatalogEntry("local");
        site.setArchitecture(mVersion.getArchitecture());
        site.addDirectory(constructFileServerDirectory(Directory.TYPE.shared_scratch, scratch));
        site.addDirectory(constructFileServerDirectory(Directory.TYPE.shared_storage, output));

        // set path to Pegasus install
        site.addProfile(new Profile(Profile.ENV, "PEGASUS_HOME", pegasusHome));

        return site;
    }

    /**
     * Constructs default SiteCatalogEntry for condorpool site
     *
     * @param options
     * @param pegasusHome the pegasus home to be set
     * @return
     */
    private SiteCatalogEntry constructDefaultCondorPoolSiteEntry(
            PlannerOptions options, String pegasusHome) {

        SiteCatalogEntry site = new SiteCatalogEntry("condorpool");
        site.setArchitecture(mVersion.getArchitecture());

        // set the profiles for the site to be treated as a condor pool
        site.addProfile(new Profile(Profile.VDS, "style", "condor"));

        return site;
    }

    /**
     * Constructs default SiteCatalogEntry for condorpool site
     *
     * @param options
     * @param pegasusHome the pegasus home to be set
     * @return
     */
    private SiteCatalogEntry constructDefaultSharedCondorPoolSiteEntry(
            PlannerOptions options, String pegasusHome) {
        String submitDir = options.getSubmitDirectory();
        File scratch = new File(new File(submitDir).getParent(), "wf-scratch/CONDORPOOL");

        SiteCatalogEntry site = new SiteCatalogEntry("condorpool-shared");
        site.setArchitecture(mVersion.getArchitecture());
        site.addDirectory(constructFileServerDirectory(Directory.TYPE.shared_scratch, scratch));

        // set path to Pegasus install
        site.addProfile(new Profile(Profile.ENV, "PEGASUS_HOME", pegasusHome));

        // set the profiles for the site to be treated as a condor pool
        site.addProfile(new Profile(Profile.VDS, "style", "condor"));
        // condorpool compute sites share a filesystem with the submit host
        site.addProfile(new Profile(Profile.VDS, Pegasus.LOCAL_VISIBLE_KEY, "true"));

        // requirements expression to pin it to a matchine
        String requirements = this.getCondorPoolRequirements();
        if (requirements != null) {
            site.addProfile(new Profile(Profile.CONDOR, Condor.REQUIREMENTS_KEY, requirements));
        }

        return site;
    }

    /**
     * Construct a file server based directory
     *
     * @param type
     * @param dir
     * @return
     */
    private Directory constructFileServerDirectory(Directory.TYPE type, File dir) {
        Map<FileServer.OPERATION, List<FileServer>> m = new HashMap();
        List<FileServer> servers = new LinkedList();
        servers.add(new FileServer("file", "file:///", dir.getAbsolutePath()));
        m.put(FileServerType.OPERATION.all, servers);
        return new Directory(
                new SharedDirectory(m, new InternalMountPoint(dir.getAbsolutePath())), type);
    }

    /**
     * Returns the requirements clause for the default condor pool site entry
     *
     * @return
     */
    private String getCondorPoolRequirements() {
        ShellCommand c = ShellCommand.getInstance(mLogger);
        if (c.execute("condor_config_val", "FULL_HOSTNAME") == 0) {
            StringBuffer requirements = new StringBuffer();
            requirements.append("(Machine == \"");
            requirements.append(c.getSTDOut());
            requirements.append("\")");
            return requirements.toString();
        }
        return null;
    }
}
