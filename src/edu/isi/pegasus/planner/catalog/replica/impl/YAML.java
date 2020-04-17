/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.isi.pegasus.planner.catalog.replica.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Escape;
import edu.isi.pegasus.common.util.VariableExpander;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaCatalogJsonDeserializer;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaCatalogKeywords;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import edu.isi.pegasus.planner.parser.YAMLSchemaValidationResult;
import edu.isi.pegasus.planner.parser.YAMLSchemaValidator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a replica catalog on top of a simple file with regular expression based
 * entries, which contains two or more columns. It is neither transactionally safe, nor advised to
 * use for production purposes in any way. Multiple concurrent instances <b>will clobber</b> each
 * other!
 *
 * <p>The site attribute should be specified whenever possible. The attribute key for the site
 * attribute is "site". For the shell planner, its value will always be "local".
 *
 * <p>The class is permissive in what inputs it accepts. The LFN may or may not be quoted. If it
 * contains linear whitespace, quotes, backslash or an equality sign, it must be quoted and escaped.
 * Ditto for the PFN.
 *
 * <p>A sample replica catalog description is indicated below.
 *
 * <p>
 *
 * <pre>
 * pegasus: "5.0"
 * replicas:
 * # matches "f.a"
 * - lfn: "f.a"
 *   pfn: "file:///Volumes/data/input/f.a"
 *   site: "local"
 *
 * # matches faa, f.a, f0a, etc.
 * - lfn: "f.a"
 *   pfn: "file:///Volumes/data/input/f.a"
 *   site: "local"
 *   regex: true
 * </pre>
 *
 * <p>The class is strict when producing (storing) results. The LFN and PFN are only quoted and
 * escaped, if necessary. The attribute values are always quoted and escaped.
 *
 * @author Karan Vahi
 * @version $Revision: 5402 $
 */
@JsonDeserialize(using = YAML.CallbackJsonDeserializer.class)
@JsonSerialize(using = YAML.JsonSerializer.class)
public class YAML implements ReplicaCatalog {
    /** The default transformation Catalog version to which this maps to */
    public static final String DEFAULT_REPLICA_CATALOG_VERSION = "5.0";

    /**
     * The name of the key that disables writing back to the cache file. Designates a static file.
     * i.e. read only
     */
    public static final String READ_ONLY_KEY = "read.only";

    /** The "not-so-official" location URL of the Replica Catalog Schema. */
    public static final String SCHEMA_URI = "http://pegasus.isi.edu/schema/rc-5.0.yml";

    /** File object of the schema.. */
    private final File SCHEMA_FILE;

    /**
     * Records the quoting mode for LFNs and PFNs. If false, only quote as necessary. If true,
     * always quote all LFNs and PFNs.
     */
    protected boolean mQuote = false;

    /** Records the name of the on-disk representation. */
    protected String mFilename = null;

    /** Maintains a memory slurp of the file representation. */
    /** Maintains a memory slurp of the file representation. */
    protected Map<String, ReplicaLocation> mLFN = null;

    protected Map<String, ReplicaLocation> mLFNRegex = null;

    protected Map<String, Pattern> mLFNPattern = null;

    /** A boolean indicating whether the catalog is read only or not. */
    boolean m_readonly;

    /** Handle to pegasus variable expander */
    private VariableExpander mVariableExpander;

    /** The version for the Replica Catalog */
    private String mVersion;

    /**
     * Default empty constructor creates an object that is not yet connected to any database. You
     * must use support methods to connect before this instance becomes usable.
     *
     * @see #connect(Properties)
     */
    public YAML() {
        // make connection defunc
        mLFN = null;
        mLFNRegex = null;
        mLFNPattern = null;
        mFilename = null;
        m_readonly = false;
        mVariableExpander = new VariableExpander();
        mVersion = YAML.DEFAULT_REPLICA_CATALOG_VERSION;

        PegasusProperties props = PegasusProperties.getInstance();
        File schemaDir = props.getSchemaDir();
        File yamlSchemaDir = new File(schemaDir, "yaml");
        SCHEMA_FILE = new File(yamlSchemaDir, new File(SCHEMA_URI).getName());
    }

    /**
     * Reads the on-disk map file into memory.
     *
     * @param filename is the name of the file to read.
     * @return true, if the in-memory data structures appear sound.
     */
    public boolean connect(String filename) {
        // sanity check
        if (filename == null) {
            return false;
        }
        mFilename = filename;
        mLFN = new LinkedHashMap<String, ReplicaLocation>();
        mLFNRegex = new LinkedHashMap<String, ReplicaLocation>();
        mLFNPattern = new LinkedHashMap<String, Pattern>();

        // first attempt to validate
        if (validate(new File(filename), SCHEMA_FILE)) {
            Reader reader = null;
            try {
                reader = new VariableExpansionReader(new FileReader(filename));
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                // inject instance of this class to be used for deserialization
                mapper.setInjectableValues(injectCallback());
                mapper.readValue(reader, YAML.class);
            } catch (IOException ioe) {
                mLFN = null;
                mLFNRegex = null;
                mLFNPattern = null;
                mFilename = null;
                throw new CatalogException(ioe); // re-throw
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                    }
                }
            }
        }
        return true;
    }

    /**
     * Establishes a connection to the database from the properties. You will need to specify a
     * "file" property to point to the location of the on-disk instance. If the property "quote" is
     * set to a true value, LFNs and PFNs are always quoted. By default, and if false, LFNs and PFNs
     * are only quoted as necessary.
     *
     * @param props is the property table with sufficient settings to establish a link with the
     *     database.
     * @return true if connected, false if failed to connect.
     * @throws Error subclasses for runtime errors in the class loader.
     */
    public boolean connect(Properties props) {
        // quote mode
        mQuote = Boolean.parse(props.getProperty("quote"));
        // update the m_writeable flag if specified
        if (props.containsKey(YAML.READ_ONLY_KEY)) {
            m_readonly = Boolean.parse(props.getProperty(YAML.READ_ONLY_KEY), false);
        }
        if (props.containsKey("file")) return connect(props.getProperty("file"));
        return false;
    }

    /**
     * Validates a file against the Replica Catalog Schema file
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
        } catch (IOException ioe) {
            throw new ReplicaCatalogException(ioe);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        JsonNode root = null;
        try {
            root = mapper.readTree(reader);
        } catch (JacksonYAMLParseException e) {
            throw new ReplicaCatalogException("Error on line " + e.getLocation().getLineNr(), e);
        } catch (Exception e) {
            throw new ReplicaCatalogException("Error in loading the yaml file " + reader, e);
        }
        if (root != null) {
            YAMLSchemaValidationResult result =
                    YAMLSchemaValidator.getInstance().validate(root, SCHEMA_FILE, "replica");

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
                throw new ReplicaCatalogException(errorResult.toString());
            }
        }
        return validate;
    }
    /**
     * Quotes a string only if necessary. This methods first determines, if a strings requires
     * quoting, because it contains whitespace, an equality sign, quotes, or a backslash. If not,
     * the string is not quoted. If the input contains forbidden characters, it is placed into
     * quotes and quote and backslash are backslash escaped.
     *
     * <p>However, if the property "quote" had a <code>true</code> value when connecting to the
     * database, output will always be quoted.
     *
     * @param e is the Escape instance used to escape strings.
     * @param s is the string that may require quoting
     * @return either the original string, or a newly allocated instance to an escaped string.
     */
    public String quote(Escape e, String s) {
        String result = null;
        if (s == null || s.length() == 0) {
            // empty string short-cut
            result = (mQuote ? "\"\"" : s);
        } else {
            // string has content
            boolean flag = mQuote;
            for (int i = 0; i < s.length() && !flag; ++i) {
                // Note: loop will never trigger, if mQuote is true
                char ch = s.charAt(i);
                flag = (ch == '"' || ch == '\\' || ch == '=' || Character.isWhitespace(ch));
            }
            result = (flag ? '"' + e.escape(s) + '"' : s);
        }
        // single point of exit
        return result;
    }

    /**
     * This operation will dump the in-memory representation back onto disk. The store operation is
     * strict in what it produces. The LFN and PFN records are only quoted, if they require quotes,
     * because they contain special characters. The attributes are <b>always</b> quoted and thus
     * quote-escaped.
     */
    public void close() {
        String newline = System.getProperty("line.separator", "\r\n");
        // sanity check
        if (mLFN == null && mLFNRegex == null) return;
        // check if the file is writeable or not
        if (m_readonly) {
            if (mLFN != null) mLFN.clear();
            mLFN = null;
            if (mLFNRegex != null) {
                mLFNRegex.clear();
                mLFNPattern.clear();
            }

            mLFNRegex = null;
            mLFNPattern = null;
            mFilename = null;
            return;
        }

        try {
            Writer out = new BufferedWriter(new FileWriter(mFilename));
            // write header
            out.write(
                    "# file-based replica catalog: "
                            + Currently.iso8601(false, true, true, new Date()));
            out.write(newline);

            // in case of yaml we write it directly to the output file so we are
            // returning null..
            ObjectMapper mapper =
                    new ObjectMapper(
                            new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
            mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
            out.write(mapper.writeValueAsString(this));
            // close
            out.close();
            // this.mFlushOnClose = false;

        } catch (IOException ioe) { // FIXME: blurt message somewhere sane
            throw new ReplicaCatalogException(
                    "Unable to write contents of Replica Catalog to " + mFilename, ioe);
        } finally {

            if (mLFN != null) {
                mLFN.clear();
            }
            mLFN = null;
            if (mLFNRegex != null) {
                mLFNRegex.clear();
                mLFNPattern.clear();
            }
            mLFNRegex = null;
            mLFNPattern = null;
            mFilename = null;
        }
    }

    private void write(Writer out, Map<String, ReplicaLocation> m) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        Escape e = new Escape("\"\\", '\\');
        if (m != null) {
            for (Iterator<String> i = m.keySet().iterator(); i.hasNext(); ) {
                String lfn = i.next();
                ReplicaLocation c = m.get(lfn);
                if (c != null) {
                    for (Iterator<ReplicaCatalogEntry> j = c.pfnIterator(); j.hasNext(); ) {
                        ReplicaCatalogEntry rce = j.next();
                        out.write(quote(e, lfn));
                        out.write(' ');
                        out.write(quote(e, rce.getPFN()));
                        for (Iterator<String> k = rce.getAttributeIterator(); k.hasNext(); ) {
                            String key = k.next();
                            String value = (String) rce.getAttribute(key);
                            out.write(' ');
                            out.write(key);
                            out.write("=\"");
                            out.write(e.escape(value));
                            out.write('"');
                        }
                        // finalize record/line
                        out.write(newline);
                    }
                }
            }
        }
    }

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mLFN == null && mLFNRegex == null);
    }

    /**
     * Retrieves the entry for a given filename and site handle from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical filename, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle) {
        Collection<ReplicaCatalogEntry> result = lookupWithHandle(lfn, handle);

        if (result == null || result.isEmpty()) {
            return null;
        }
        return result.iterator().next().getPFN();
    }

    public Collection<ReplicaCatalogEntry> lookupWithHandle(String lfn, String handle) {
        Collection<ReplicaCatalogEntry> c = new ArrayList<ReplicaCatalogEntry>();

        // Lookup regular LFN's
        ReplicaLocation tmp = mLFN.get(lfn);
        if (tmp != null) {
            for (ReplicaCatalogEntry rce : tmp.getPFNList()) {
                String pool = rce.getResourceHandle();
                if (pool == null && handle == null
                        || pool != null && handle != null && pool.equals(handle)) c.add(rce);
            }
        }

        // Lookup regex LFN's
        Pattern p = null;
        Matcher m = null;
        String pool = null;
        ReplicaCatalogEntry rce = null;
        for (String l : mLFNRegex.keySet()) {
            p = mLFNPattern.get(l);
            m = p.matcher(lfn);
            if (m.matches()) {
                ReplicaLocation entries = mLFNRegex.get(l);
                for (ReplicaCatalogEntry entry : entries.getPFNList()) {
                    pool = entry.getResourceHandle();
                    if (pool == null && handle == null
                            || pool != null && handle != null && pool.equals(handle)) {
                        String tmpPFN = entry.getPFN();
                        for (int k = 0, j = m.groupCount(); k <= j; ++k) {
                            // tmpPFN = tmpPFN.replaceAll ("\\$" + k, m.group (k));
                            tmpPFN = tmpPFN.replaceAll("\\[" + k + "\\]", m.group(k));
                        }
                        if (tmpPFN.indexOf('[') >= 0) {
                            // PFN still has variables left.
                        }
                        // Add new RCE
                        rce = cloneRCE(entry);
                        rce.setPFN(tmpPFN);
                        c.add(rce);
                    }
                }
            }
        }

        return c;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a collection of replica catalog entries
     * @see ReplicaCatalogEntry
     */
    public Collection<ReplicaCatalogEntry> lookup(String lfn) {
        Collection<ReplicaCatalogEntry> c = new ArrayList<ReplicaCatalogEntry>();
        ReplicaLocation tmp;
        // Lookup regular LFN's
        tmp = mLFN.get(lfn);
        if (tmp != null) c.addAll(tmp.getPFNList());
        // Lookup regex LFN's
        ReplicaCatalogEntry rce = null;
        Pattern p = null;
        Matcher m = null;
        for (String l : mLFNRegex.keySet()) {
            p = mLFNPattern.get(l);
            m = p.matcher(lfn);
            if (m.matches()) {
                ReplicaLocation entries = mLFNRegex.get(l);
                Collection<ReplicaCatalogEntry> entriesResult =
                        new ArrayList<ReplicaCatalogEntry>();
                for (ReplicaCatalogEntry entry : entries.getPFNList()) {
                    String tmpPFN = entry.getPFN();
                    for (int k = 0, j = m.groupCount(); k <= j; ++k) {
                        tmpPFN = tmpPFN.replaceAll("\\[" + k + "\\]", m.group(k));
                    }
                    if (tmpPFN.indexOf('[') >= 0) {
                        // PFN still has variables left.
                    }
                    // Add new RCE
                    rce = cloneRCE(entry);
                    rce.setPFN(tmpPFN);
                    entriesResult.add(rce);
                }
                c.addAll(entriesResult);
                break;
            }
        }
        return c;
    }

    private ReplicaCatalogEntry cloneRCE(ReplicaCatalogEntry e) {

        return (ReplicaCatalogEntry) e.clone();
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is just a PFN string. Duplicates are reduced through the set paradigm.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a set of PFN strings
     */
    public Set<String> lookupNoAttributes(String lfn) {
        Collection<ReplicaCatalogEntry> input = lookup(lfn);
        Set<String> result = new HashSet<String>();
        if (input == null || input.size() == 0) return result;
        for (ReplicaCatalogEntry entry : input) {
            result.add(entry.getPFN());
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries for
     *     the LFN.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public Map lookup(Set lfns) {
        Map<String, Collection<ReplicaCatalogEntry>> result =
                new HashMap<String, Collection<ReplicaCatalogEntry>>();
        if (lfns == null || lfns.size() == 0) return result;
        Collection<ReplicaCatalogEntry> c = null;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            String lfn = i.next();
            c = lookup(lfn);
            result.put(lfn, new ArrayList<ReplicaCatalogEntry>(c));
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns) {
        Map<String, Collection<ReplicaCatalogEntry>> input = lookup(lfns);
        if (input == null || input.size() == 0) return input;
        Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
        for (Map.Entry<String, Collection<ReplicaCatalogEntry>> entry : input.entrySet()) {
            String lfn = entry.getKey();
            Collection<ReplicaCatalogEntry> c = entry.getValue();
            Set<String> value = new HashSet<String>();
            if (c != null) {
                for (Iterator<ReplicaCatalogEntry> j = c.iterator(); j.hasNext(); ) {
                    value.add(j.next().getPFN());
                }
            }
            result.put(lfn, value);
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries (all
     *     attributes).
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String handle) {
        ReplicaLocation rl = null;
        Pattern p = null;
        Matcher m = null;
        String lfn = null;
        String pool = null;
        Map<String, Collection<ReplicaCatalogEntry>> result =
                new HashMap<String, Collection<ReplicaCatalogEntry>>();
        ReplicaCatalogEntry rce = null;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            lfn = i.next(); // f.a - String file name
            List<ReplicaCatalogEntry> value = new ArrayList<ReplicaCatalogEntry>();
            // Lookup regular LFN's
            rl = mLFN.get(lfn);
            if (rl != null) {
                for (Iterator j = rl.pfnIterator(); j.hasNext(); ) {
                    rce = (ReplicaCatalogEntry) j.next();
                    pool = rce.getResourceHandle();
                    if (pool == null && handle == null
                            || pool != null && handle != null && pool.equals(handle))
                        value.add(rce);
                }
            }
            // Lookup regex LFN's
            for (String l : mLFNRegex.keySet()) {
                p = mLFNPattern.get(l); // Get one pattern
                m = p.matcher(lfn); // See if f.a matches pattern
                if (m.matches()) // Pattern matches?
                {
                    ReplicaLocation entries = mLFNRegex.get(l);
                    // Get all RCE entries for the matched pattern.
                    for (ReplicaCatalogEntry entry : entries.getPFNList()) {
                        pool = entry.getResourceHandle();
                        // Entry matches handle requirement?
                        if (pool == null && handle == null
                                || pool != null && handle != null && pool.equals(handle)) {
                            String tmpPFN = entry.getPFN();
                            // Substitute variables in PFN before returning
                            for (int k = 0, j = m.groupCount(); k <= j; ++k) {
                                tmpPFN = tmpPFN.replaceAll("\\[" + k + "\\]", m.group(k));
                            }
                            // Are there unsubstituted variables?
                            if (tmpPFN.indexOf('[') >= 0) {
                                // PFN still has variables left.
                            }
                            // Return new PFN
                            // entry.setPFN( tmpPFN );
                            rce = cloneRCE(entry);
                            rce.setPFN(tmpPFN);
                            value.add(rce);
                            // Break if value.size == 2?
                        }
                    }
                    break;
                }
            }
            result.put(lfn, value);
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a set of physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle) {
        Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
        if (lfns == null || lfns.size() == 0) return result;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            String lfn = i.next();
            Collection<ReplicaCatalogEntry> c = lookupWithHandle(lfn, handle);
            if (c != null) {
                List<String> value = new ArrayList<String>();
                for (ReplicaCatalogEntry entry : c) {
                    value.add(entry.getPFN());
                }
                result.put(lfn, value);
            }
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any attribute name, e.g. the resource
     *     handle 'pool', to a string that has some meaning to the implementing system. This can be
     *     a SQL wildcard for queries, or a regular expression for Java-based memory collections.
     *     Unknown keys are ignored. Using an empty map requests the complete catalog.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries.
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Map constraints) {
        if (constraints == null || constraints.size() == 0) {
            // return everything
            // Map<String, Collection<ReplicaCatalogEntry>> result =
            //        new HashMap<String, Collection<ReplicaCatalogEntry>>(mLFN);
            // result.putAll(mLFNRegex);
            Map<String, Collection<ReplicaCatalogEntry>> result =
                    new HashMap<String, Collection<ReplicaCatalogEntry>>();
            for (Map.Entry<String, ReplicaLocation> entry : mLFN.entrySet()) {
                ReplicaLocation rl = entry.getValue();
                result.put(entry.getKey(), rl.getPFNList());
            }
            for (Map.Entry<String, ReplicaLocation> entry : mLFNRegex.entrySet()) {
                ReplicaLocation rl = entry.getValue();
                result.put(entry.getKey(), rl.getPFNList());
            }
            return Collections.unmodifiableMap(result);
        } else if (constraints.size() == 1 && constraints.containsKey("lfn")) {
            // return matching LFNs
            Pattern p = Pattern.compile((String) constraints.get("lfn"));
            Map<String, Collection<ReplicaCatalogEntry>> result =
                    new HashMap<String, Collection<ReplicaCatalogEntry>>();
            for (Iterator<Entry<String, ReplicaLocation>> i = mLFN.entrySet().iterator();
                    i.hasNext(); ) {
                Entry<String, ReplicaLocation> e = i.next();
                String lfn = e.getKey();
                if (p.matcher(lfn).matches()) result.put(lfn, e.getValue().getPFNList());
            }

            return result;
        } else {
            // FIXME: Implement!
            throw new RuntimeException("method not implemented");
        }
    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     */
    public Set list() {
        Set<String> s = new HashSet<String>(mLFN.keySet());
        s.addAll(mLFNRegex.keySet());
        return s;
    }

    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical filename only. It is a string that has some
     *     meaning to the implementing system. This can be a SQL wildcard for queries, or a regular
     *     expression for Java-based memory collections.
     * @return A set of logical filenames that match. The set may be empty
     */
    public Set list(String constraint) {
        Set<String> result = new HashSet<String>();
        Pattern p = Pattern.compile(constraint);
        for (Iterator<String> i = list().iterator(); i.hasNext(); ) {
            String lfn = i.next();
            if (p.matcher(lfn).matches()) result.add(lfn);
        }
        // done
        return result;
    }

    /**
     * Inserts a new mapping into the replica catalog. Any existing mapping of the same LFN, PFN,
     * and HANDLE will be replaced, including all of its attributes.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        if (lfn == null || tuple == null) throw new NullPointerException();

        boolean isRegex = tuple.isRegex();
        ReplicaLocation rl = null;

        // Collection<ReplicaCatalogEntry> c = null;

        String pfn = tuple.getPFN();
        String handle = tuple.getResourceHandle();

        if (mLFN.containsKey(lfn)) {
            rl = mLFN.get(lfn);
            Collection<ReplicaCatalogEntry> c = rl.getPFNList();

            for (Iterator<ReplicaCatalogEntry> i = c.iterator(); i.hasNext(); ) {
                ReplicaCatalogEntry rce = i.next();

                if (pfn.equals(rce.getPFN())
                        && ((handle == null && rce.getResourceHandle() == null)
                                || (handle != null && handle.equals(rce.getResourceHandle())))) {
                    try {
                        i.remove();
                        break;
                    } catch (UnsupportedOperationException uoe) {
                        return 0;
                    }
                }
            }
        }

        if (mLFNRegex.containsKey(lfn)) {
            rl = mLFNRegex.get(lfn);
            Collection<ReplicaCatalogEntry> c = rl.getPFNList();

            for (Iterator<ReplicaCatalogEntry> i = c.iterator(); i.hasNext(); ) {
                ReplicaCatalogEntry rce = i.next();

                if (pfn.equals(rce.getPFN())
                        && ((handle == null && rce.getResourceHandle() == null)
                                || (handle != null && handle.equals(rce.getResourceHandle())))) {
                    try {
                        i.remove();
                        break;
                    } catch (UnsupportedOperationException uoe) {
                        return 0;
                    }
                }
            }
        }

        rl = isRegex ? mLFNRegex.get(lfn) : mLFN.get(lfn);
        Collection<ReplicaCatalogEntry> c = null;
        if (rl != null) {
            c = rl.getPFNList();
        }
        // c = isRegex ? mLFNRegex.get(lfn).getPFNList():mLFN.get(lfn).getPFNList();

        c = (c == null) ? new ArrayList<ReplicaCatalogEntry>() : c;

        c.add(tuple);
        if (isRegex) {
            mLFNRegex.put(lfn, new ReplicaLocation(lfn, c, false));
            mLFNPattern.put(lfn, Pattern.compile(lfn));
        } else {
            mLFN.put(lfn, new ReplicaLocation(lfn, c, false));
        }

        return 1;
    }

    /**
     * Inserts a new mapping into the replica catalog. This is a convenience function exposing the
     * resource handle. Internally, the <code>ReplicaCatalogEntry</code> element will be
     * constructed, and passed to the appropriate insert function.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see #insert(String, ReplicaCatalogEntry)
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        if (lfn == null || pfn == null || handle == null) throw new NullPointerException();
        return insert(lfn, new ReplicaCatalogEntry(pfn, handle));
    }

    /**
     * Inserts multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. Note that this
     * operation will replace existing entries.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @return the number of insertions.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public int insert(Map x) {
        int result = 0;
        // shortcut sanity
        if (x == null || x.size() == 0) return result;
        for (Iterator<String> i = x.keySet().iterator(); i.hasNext(); ) {
            String lfn = i.next();
            Object val = x.get(lfn);
            if (val instanceof ReplicaCatalogEntry) {
                // permit misconfigured clients
                result += insert(lfn, (ReplicaCatalogEntry) val);
            } else {
                // this is how it should have been
                for (Iterator j = ((Collection) val).iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    result += insert(lfn, rce);
                }
            }
        }
        return result;
    }

    /**
     * Deletes a specific mapping from the replica catalog. We don't care about the resource handle.
     * More than one entry could theoretically be removed. Upon removal of an entry, all attributes
     * associated with the PFN also evaporate (cascading deletion).
     *
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn) {
        throw new java.lang.UnsupportedOperationException(
                "delete(String,String) not implemented as yet");
    }

    /**
     * Deletes multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. On setting
     * matchAttributes to false, all entries having matching lfn pfn mapping to an entry in the Map
     * are deleted. However, upon removal of an entry, all attributes associated with the pfn also
     * evaporate (cascaded deletion).
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @param matchAttributes whether mapping should be deleted only if all attributes match.
     * @return the number of deletions.
     * @see ReplicaCatalogEntry
     */
    public int delete(Map x, boolean matchAttributes) {
        throw new java.lang.UnsupportedOperationException(
                "delete(Map,boolean) not implemented as yet");
    }

    /**
     * Attempts to see, if all keys in the partial replica catalog entry are contained in the full
     * replica catalog entry.
     *
     * @param full is the full entry to check against.
     * @param part is the partial entry to check with.
     * @return true, if contained, false if not contained.
     */
    private boolean matchMe(ReplicaCatalogEntry full, ReplicaCatalogEntry part) {
        if (full.getPFN().equals(part.getPFN())) {
            for (Iterator<String> i = part.getAttributeIterator(); i.hasNext(); ) {
                if (!full.hasAttribute(i.next())) return false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Deletes a very specific mapping from the replica catalog. The LFN must be matches, the PFN,
     * and all PFN attributes specified in the replica catalog entry. More than one entry could
     * theoretically be removed. Upon removal of an entry, all attributes associated with the PFN
     * also evaporate (cascading deletion).
     *
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        throw new java.lang.UnsupportedOperationException(
                "delete(String, ReplicaCatalogEntry) not implemented as yet");
    }

    /**
     * Looks for a match of an attribute value in a replica catalog entry.
     *
     * @param rce is the replica catalog entry
     * @param name is the attribute key to match
     * @param value is the value to match against
     * @return true, if a match was found.
     */
    /*
     * private boolean hasMatchingAttr (ReplicaCatalogEntry rce, String name,
     * Object value) { if (rce.hasAttribute (name)) return rce.getAttribute
     * (name).equals (value); else return value == null; }
     */

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the PFN attribute is
     * found, and matches exactly the object value. This method may be useful to remove all replica
     * entries that have a certain MD5 sum associated with them. It may also be harmful overkill.
     *
     * @param lfn is the logical filename to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value) {
        throw new java.lang.UnsupportedOperationException(
                "delete (String lfn, String name, Object value) not implemented as yet");
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the resource handle is
     * found. Karan requested this convenience method, which can be coded like
     *
     * <p>
     *
     * <pre>
     * delete( lfn, RESOURCE_HANDLE, handle )
     * </pre>
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle) {
        throw new java.lang.UnsupportedOperationException(
                "deleteByResource (String lfn, String handle) not implemented as yet");
    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        throw new java.lang.UnsupportedOperationException(
                "remove (String lfn) not implemented as yet");
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     * @return the number of removed entries.
     * @see #remove(String)
     */
    public int remove(Set lfns) {
        throw new java.lang.UnsupportedOperationException(
                "remove (Set lfns) not implemented as yet");
    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute is found, and matches
     * exactly the object value.
     *
     * @param name is the PFN attribute key to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value) {
        throw new java.lang.UnsupportedOperationException(
                "removeByAttribute (String lfn, Object value) not implemented as yet");
    }

    /**
     * Removes all entries associated with a particular resource handle. This is useful, if a site
     * goes offline. It is a convenience method, which calls the generic <code>removeByAttribute
     * </code> method.
     *
     * @param handle is the site handle to remove all entries for.
     * @return the number of removed entries.
     * @see #removeByAttribute(String, Object)
     */
    public int removeByAttribute(String handle) {
        throw new java.lang.UnsupportedOperationException(
                "removeByAttribute (String handle) not implemented as yet");
    }

    /**
     * Set the Catalog version
     *
     * @param version
     */
    public void setVersion(String version) {
        this.mVersion = version;
    }

    /**
     * Get the Catalog version
     *
     * @return version
     */
    public String getVersion() {
        return this.mVersion;
    }

    /**
     * Removes everything. Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear() {
        int result = mLFN.size() + mLFNRegex.size();
        mLFN.clear();
        mLFNRegex.clear();
        mLFNPattern.clear();
        return result;
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        return new java.io.File(this.mFilename);
    }

    /**
     * Set the catalog to read-only mode.
     *
     * @param readonly whether the catalog is read-only
     */
    @Override
    public void setReadOnly(boolean readonly) {
        this.m_readonly = readonly;
    }

    /**
     * Set the Callback as an injectable value to insert into the Deserializer via Jackson.
     *
     * @return
     */
    private InjectableValues injectCallback() {
        return new InjectableValues.Std().addValue("callback", this);
    }

    /**
     * Custom deserializer for YAML representation of Replica Catalog that calls back to the class
     * that invoked the serializer. The deserialized object returned is the callback itself
     *
     * @author Karan Vahi
     */
    static class CallbackJsonDeserializer extends ReplicaCatalogJsonDeserializer<ReplicaCatalog> {

        /**
         * Deserializes a Transformation YAML description of the type
         *
         * <pre>
         *  pegasus: 5.0
         *  replicas:
         *    # matches "f.a"
         *    - lfn: "f.a"
         *      pfn: "file:///Volumes/data/input/f.a"
         *      site: "local"
         *
         *    # matches faa, f.a, f0a, etc.
         *    - lfn: "f.a"
         *    pfn: "file:///Volumes/data/input/f.a"
         *    site: "local"
         *    regex: true
         * </pre>
         *
         * @param parser
         * @param dc
         * @return
         * @throws IOException
         * @throws JsonProcessingException
         */
        @Override
        public ReplicaCatalog deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);
            YAML yamlRC = (YAML) dc.findInjectableValue("callback", null, null);
            if (yamlRC == null) {
                throw new RuntimeException("Callback not initialized when parsing inititated");
            }
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
                }

                String keyValue = node.get(key).asText();
                switch (reservedKey) {
                    case PEGASUS:
                        yamlRC.setVersion(keyValue);
                        break;

                    case REPLICAS:
                        JsonNode replicaNodes = node.get(key);
                        if (replicaNodes != null) {
                            if (replicaNodes.isArray()) {
                                for (JsonNode replicaNode : replicaNodes) {
                                    ReplicaLocation rl = this.createReplicaLocation(replicaNode);
                                    int count = rl.getPFNCount();
                                    if (count == 0 || count > 1) {
                                        throw new ReplicaCatalogException(
                                                "ReplicaLocation for ReplicaLocation "
                                                        + rl
                                                        + " can only have one pfn. Found "
                                                        + count);
                                    }
                                    ReplicaCatalogEntry rce = rl.getPFNList().get(0);
                                    yamlRC.insert(rl.getLFN(), rce);
                                }
                            }
                        }
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
                }
            }

            return yamlRC;
        }
    }

    /**
     * Custom serializer for YAML representation of Transformation Catalog Entry
     *
     * @author Karan Vahi
     */
    static class JsonSerializer extends PegasusJsonSerializer<YAML> {

        public JsonSerializer() {}

        /**
         * Serializes contents into YAML representation
         *
         * @param entry
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(YAML catalog, JsonGenerator gen, SerializerProvider sp)
                throws IOException {

            gen.writeStartObject();
            writeStringField(
                    gen, ReplicaCatalogKeywords.PEGASUS.getReservedName(), catalog.getVersion());

            gen.writeArrayFieldStart(ReplicaCatalogKeywords.REPLICAS.getReservedName());

            // first serialize the non regex entries
            for (ReplicaLocation rl : catalog.mLFN.values()) {
                serialize(rl, gen, sp);
            }
            // serialize the regex entries
            for (ReplicaLocation rl : catalog.mLFNRegex.values()) {
                serialize(rl, gen, sp);
            }

            gen.writeEndArray();
            gen.writeEndObject();
        }

        /**
         * Serializes contents into YAML representation
         *
         * @param entry
         * @param gen
         * @param sp
         * @throws IOException
         */
        private void serialize(ReplicaLocation rl, JsonGenerator gen, SerializerProvider sp)
                throws IOException {

            for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                gen.writeStartObject();
                
                writeStringField(gen, ReplicaCatalogKeywords.LFN.getReservedName(), rl.getLFN());
                writeStringField(gen, ReplicaCatalogKeywords.PFN.getReservedName(), rce.getPFN());
                writeStringField(
                        gen,
                        ReplicaCatalogKeywords.SITE.getReservedName(),
                        rce.getResourceHandle());
                if(rce.isRegex()){
                    writeStringField(gen, ReplicaCatalogKeywords.REGEX.getReservedName(), "true");
                }
                //if(rce.hasAttribute(ReplicaCatalog.))
               // gen.writeFieldName(TransformationCatalogKeywords.PROFILES.getReservedName());
                //    gen.writeObject(entry.getAllProfiles());
                gen.writeEndObject();
            }
        }
    }
}
