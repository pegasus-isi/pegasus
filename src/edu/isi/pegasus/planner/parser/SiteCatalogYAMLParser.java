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

package edu.isi.pegasus.planner.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;
import edu.isi.pegasus.planner.catalog.site.classes.Connection;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory.TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.JOB_TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.SCHEDULER_TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteDataVisitor;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.XML4PrintVisitor;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class parses and validates Site Catalog in YAML format corresponding to
 * Site Catalog schema v5.0
 *
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteCatalogYAMLParser {

    /** The "not-so-official" location URL of the Site Catalog Schema. */
    public static final String SCHEMA_URI = "http://pegasus.isi.edu/schema/sc-5.0.yml";

    
    /** The final result constructed. */
    private SiteStore mResult;

    /** The set of sites that need to be parsed. */
    private Set<String> mSites;

    /** A boolean indicating whether to load all sites. */
    private boolean mLoadAll;

    /** flag to denote if the parsing is done or not */
    private boolean mParsingDone = false;

    /** Logger for logging the properties.. */
    private final LogManager mLogger;

    /** Holder for various Pegasus properties.. */
    private final PegasusProperties mProps;

    /** File object of the schema.. */
    private final File SCHEMA_FILENAME;

    /**
     * The constructor.
     *
     * @param bag the bag of initialization objects.
     * @param sites the list of sites that need to be parsed. * means all.
     */
    public SiteCatalogYAMLParser(PegasusBag bag, List<String> sites) {
        mSites = new HashSet<String>();
        for (Iterator<String> it = sites.iterator(); it.hasNext(); ) {
            mSites.add(it.next());
        }
        mLoadAll = mSites.contains("*");
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
        File schemaDir = this.mProps.getSchemaDir();
        File yamlSchemaDir = new File(schemaDir, "yaml");
        SCHEMA_FILENAME = new File(yamlSchemaDir, new File(SCHEMA_URI).getName());
    }

    /**
     * Returns the constructed site store object
     *
     * @return <code>SiteStore<code> if parsing completed
     */
    public SiteStore getSiteStore() {
        if (mParsingDone) {
            return mResult;
        } else {
            throw new RuntimeException(
                    "Parsing of file needs to complete before function can be called");
        }
    }

    /**
     * The main method that starts the parsing.
     *
     * @param file the YAML file to be parsed.
     */
    @SuppressWarnings("unchecked")
    public void startParser(String file) {
        try {

            File f = new File(file);
            mResult = new SiteStore();
            if (!(f.exists() && f.length() > 0)) {
                mLogger.log(
                        "The Site Catalog file " + file + " was not found or empty",
                        LogManager.INFO_MESSAGE_LEVEL);
                mParsingDone = true;
                return;
            }
            
            //first attempt to validate
            if (validate(f, SCHEMA_FILENAME)) {
                //validation succeeded. load.
                Reader reader = new VariableExpansionReader(new FileReader(f));
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                SiteStore store = mapper.readValue(reader, SiteStore.class);
                for (Iterator<SiteCatalogEntry> it= store.entryIterator(); it.hasNext(); ){
                    SiteCatalogEntry entry = it.next();
                    if (loadSite(entry)) {
                        mResult.addEntry(entry);
                    }
                }
            }
        } catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        }
        mParsingDone = true;
    }

    /**
     * Whether to laod a site or not in the <code>SiteStore</code>
     *
     * @param site the <code>SiteCatalogEntry</code> object.
     * @return boolean
     */
    private boolean loadSite(SiteCatalogEntry site) {
        return (mLoadAll || mSites.contains(site.getSiteHandle()));
    }

    /**
     * Returns a list of profiles that have to be applied to the entries for all the sites
     * corresponding to a transformation.
     *
     * @param metaObj
     * @param profileObj
     * @return Profiles specified
     * @throws IOException
     * @throws ScannerException
     */
    @SuppressWarnings("unchecked")
    private Profiles getProfilesForTransformation(Object profileObj, Object metaObj) {
        Profiles profiles = new Profiles();
        if (profileObj != null) {
            List<Object> profileInfo = (List<Object>) profileObj;
            for (Object profile : profileInfo) {
                Map<String, Object> profileMaps = (Map<String, Object>) profile;
                for (Entry<String, Object> profileMapsEntries : profileMaps.entrySet()) {
                    String profileName = profileMapsEntries.getKey();
                    if (Profile.namespaceValid(profileName)) {
                        Map<String, String> profileMap =
                                (Map<String, String>) profileMapsEntries.getValue();
                        for (Entry<String, String> profileMapEntries : profileMap.entrySet()) {
                            Object key = profileMapEntries.getKey();
                            Object value = profileMapEntries.getValue();
                            profiles.addProfile(
                                    new Profile(
                                            profileName,
                                            niceString(String.valueOf(key)),
                                            niceString(String.valueOf(value))));
                        }
                    }
                }
            }
        }
        if (metaObj != null) {
            Map<String, String> metaMap = (Map<String, String>) metaObj;
            for (Entry<String, String> profileMapEntries : metaMap.entrySet()) {
                Object key = profileMapEntries.getKey();
                Object value = profileMapEntries.getValue();
                profiles.addProfile(
                        new Profile(
                                Profile.METADATA,
                                niceString(String.valueOf(key)),
                                niceString(String.valueOf(value))));
            }
        }
        return profiles;
    }

    /**
     * Remove potential leading and trainling quotes from a string.
     *
     * @param input is a string which may have leading and trailing quotes
     * @return a string that is either identical to the input, or a substring thereof.
     */
    public String niceString(String input) {
        // sanity
        if (input == null) {
            return input;
        }
        int l = input.length();
        if (l < 2) {
            return input;
        }

        // check for leading/trailing quotes
        if (input.charAt(0) == '"' && input.charAt(l - 1) == '"') {
            return input.substring(1, l - 1);
        } else {
            return input;
        }
    }

    /**
     * This method is used to extract the necessary information from the parsing exception
     *
     * @param e The parsing exception generated from the yaml.
     * @return String representing the line number and the problem is returned
     */
    private String parseError(JacksonYAMLParseException e) {
        /*
        StringBuilder builder = new StringBuilder();
        builder.append("Problem in the line :" + (e.getProblemMark().getLine() + 1) + ", column:"
        		+ e.getProblemMark().getColumn() + " with tag "
        		+ e.getProblemMark().get_snippet().replaceAll("\\s", ""));
        return builder.toString();
                  */
        return e.toString();
    }

   
    /** @param args */
    public static void main(String[] args) {
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        logger.setLevel(5);
        logger.logEventStart("test.parser", "dax", null);

        List s = new LinkedList();
        s.add("*");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        List<String> files = new LinkedList<String>();
        files.add(
                "/home/mukund/pegasus/test/junit/edu/isi/pegasus/planner/catalog/site/impl/input/sites.yaml");

        for (String file : files) {
            SiteCatalogYAMLParser parser = new SiteCatalogYAMLParser(bag, s);
            System.out.println(" *********Parsing File *********" + file);
            parser.startParser(file);
            SiteStore store = parser.getSiteStore();
            // System.out.println( store );

            SiteCatalogEntry entry = store.lookup("local");

            System.out.println(entry);
            SiteDataVisitor visitor = new XML4PrintVisitor();
            StringWriter writer = new StringWriter();
            visitor.initialize(writer);

            try {
                store.accept(visitor);
                System.out.println("Site Catalog is \n" + writer.toString());
            } catch (IOException ex) {
                Logger.getLogger(SiteCatalogYAMLParser.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println(" *********Parsing Done *********");
        }

        //        System.out.println( Directory.TYPE.value( "shared-scratch" ));
    }

    /**
     * Validates a file against the Site Catalog Schema file
     * 
     * @param f
     * @param schemaFile
     * @return 
     */
    protected boolean validate(File f, File schemaFile) {
        boolean validate = true;
        Reader reader = null;
        try {
            reader = new VariableExpansionReader(new FileReader(f));
        }
        catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        }
        
        
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        JsonNode root = null;
        try {
            root = mapper.readTree(reader);

        } catch (JacksonYAMLParseException e) {
            throw new ScannerException(e.getLocation().getLineNr(), parseError(e));
        } catch (Exception e) {
            throw new ScannerException("Error in loading the yaml file " + reader, e);
        }
        if (root != null) {
            YAMLSchemaValidationResult result =
                    YAMLSchemaValidator.getInstance()
                            .validate(root, SCHEMA_FILENAME, "site");

            // schema validation is done here.. in case of any validation error we throw the
            // result..
            if (!result.isSuccess()) {
                List<String> errors = result.getErrorMessage();
                StringBuilder errorResult = new StringBuilder();
                int i = 1;
                for (String error : errors) {
                    if (i > 1) {
                        errorResult.append(",");
                    }
                    errorResult.append("Error ").append(i++).append(":{");
                    errorResult.append(error).append("}");
                }
                throw new ScannerException(errorResult.toString());
            }
        }
        return validate;
    }
}
