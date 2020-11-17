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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class parses and validates Site Catalog in YAML format corresponding to Site Catalog schema
 * v5.0
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteCatalogYAMLParser extends YAMLParser {

    /** The "not-so-official" location URL of the Site Catalog Schema. */
    public static final String SCHEMA_URI = "https://pegasus.isi.edu/schema/sc-5.0.yml";

    /** The final result constructed. */
    private SiteStore mResult;

    /** The set of sites that need to be parsed. */
    private Set<String> mSites;

    /** A boolean indicating whether to load all sites. */
    private boolean mLoadAll;

    /** flag to denote if the parsing is done or not */
    private boolean mParsingDone = false;

    /** File object of the schema.. */
    private final File SCHEMA_FILENAME;

    /**
     * The constructor.
     *
     * @param bag the bag of initialization objects.
     * @param sites the list of sites that need to be parsed. * means all.
     */
    public SiteCatalogYAMLParser(PegasusBag bag, List<String> sites) {
        super(bag);
        mSites = new HashSet<String>();
        for (Iterator<String> it = sites.iterator(); it.hasNext(); ) {
            mSites.add(it.next());
        }
        mLoadAll = mSites.contains("*");
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

            // first attempt to validate
            if (validate(f, SCHEMA_FILENAME, "site")) {
                // validation succeeded. load.
                Reader reader = new VariableExpansionReader(new FileReader(f));
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                SiteStore store = mapper.readValue(reader, SiteStore.class);
                for (Iterator<SiteCatalogEntry> it = store.entryIterator(); it.hasNext(); ) {
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
}
