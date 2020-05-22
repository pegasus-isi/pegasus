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
package edu.isi.pegasus.common.util;

import com.google.common.base.Charsets;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class creates a common interface to handle package properties. The package properties are
 * meant as read-only (so far, until requirements crop up for write access). The class is
 * implemented as a Singleton pattern.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @author Karan Vahi
 * @author Mats Rynge
 * @version $Revision$
 */
public class CommonProperties implements Cloneable {
    /** implements the singleton access via class variable. */
    private static CommonProperties m_instance = null;

    /** internal set of properties. Direct access is expressly forbidden. */
    private Properties m_props;

    /** The bin dir of the Pegasus install */
    private File m_binDir;

    /**
     * GNU: read-only single-machine data in DIR [PREFIX/etc]. The files in this directory have a
     * low change frequency, are effectively read-only, they reside on a per-machine basis, and they
     * are usually valid for a single user.
     */
    private File m_sysConfDir;

    /**
     * GNU: modifiable architecture-independent data in DIR [PREFIX/share/pegasus]. The files in
     * this directory have a high change frequency, are effectively read-write, can be shared via a
     * networked FS, and they are usually valid for multiple users.
     */
    private File m_sharedStateDir;

    /** Location of our schemas */
    private File m_schemaDir;

    /** Basename of the file to read to obtain system properties */
    public static final String PROPERTY_FILENAME = "properties";

    /** Basename of the (new) file to read for user properties. */
    public static final String USER_PROPERTY_FILENAME = ".pegasusrc";

    /** Default encoding set to be used for properties * */
    public static final Charset DEFAULT_ENCODING_SET = Charsets.UTF_8;

    /**
     * Adds new properties to an existing set of properties while substituting variables. This
     * function will allow value substitutions based on other property values. Value substitutions
     * may not be nested. A value substitution will be ${property.key}, where the dollar-brace and
     * close-brace are being stripped before looking up the value to replace it with. Note that the
     * ${..} combination must be escaped from the shell.
     *
     * @param a is the initial set of known properties (besides System ones)
     * @param b is the set of properties to add to a
     * @return the combined set of properties from a and b.
     */
    protected static Properties addProperties(Properties a, Properties b) {
        // initial
        Properties result = new Properties(a);
        Properties sys = System.getProperties();
        Pattern pattern = Pattern.compile("\\$\\{[-a-zA-Z0-9._]+\\}");

        for (Enumeration e = b.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            String value = b.getProperty(key);

            // unparse value ${prop.key} inside braces
            Matcher matcher = pattern.matcher(value);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                // extract name of properties from braces
                String newKey = value.substring(matcher.start() + 2, matcher.end() - 1);

                // try to find a matching value in result properties
                String newVal = result.getProperty(newKey);

                /*
                 * // if not found, try b's properties
                 * if ( newVal == null ) newVal = b.getProperty(newKey);
                 */

                // try myself
                if (newVal == null) newVal = result.getProperty(newKey);

                // if still not found, try system properties
                if (newVal == null) newVal = sys.getProperty(newKey);

                // replace braced string with the actual value or empty string
                matcher.appendReplacement(sb, newVal == null ? "" : newVal);
            }
            matcher.appendTail(sb);
            result.setProperty(key, sb.toString());
        }

        // final
        return result;
    }

    /**
     * Set some defaults, should values be missing in the dataset.
     *
     * @return the properties.
     */
    private static Properties defaultProps() {
        // initial
        Properties result = new Properties();

        // copy pegasus keys as specified in the system properties to defaults
        Properties sys = System.getProperties();
        for (Enumeration e = sys.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            if (key.startsWith("pegasus.")) result.setProperty(key, sys.getProperty(key));
        }

        // INSERT HERE!

        // final
        return addProperties(new Properties(), result);
    }

    /**
     * ctor. This initializes the local instance of properties from a central file.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     * @exception IOException will be thrown if reading the property file goes awry.
     * @exception MissingResourceException will be thrown if not all required properties are set
     */
    protected CommonProperties(String confProperties) throws IOException, MissingResourceException {

        // create empty new instance
        this.m_props = new Properties(defaultProps());

        // first check for old -D option - this is just a warning
        if (System.getProperty("pegasus.properties") != null) {
            File props = new File(System.getProperty("pegasus.properties"));
            if (props.exists()) {
                System.err.println(
                        "[WARNING] Properties are no longer loaded from by"
                                + " -Dpegasus.properties property. "
                                + props.getAbsolutePath()
                                + " will not be loaded. Use --conf option instead.");
            }
        }

        // first check for old -D option - this is just a warning
        if (System.getProperty("pegasus.user.properties") != null) {
            File props = new File(System.getProperty("pegasus.user.properties"));
            if (props.exists()) {
                System.err.println(
                        "[WARNING] Properties are no longer loaded by "
                                + "specifying -Dpegasus.user.properties property. "
                                + props.getAbsolutePath()
                                + " will not be loaded. Use --conf option instead.");
            }
        }

        // add user properties afterwards to have higher precedence
        String userHome = System.getProperty("user.home", ".");

        // try loading $HOME/.pegasusrc
        File props = new File(userHome, CommonProperties.USER_PROPERTY_FILENAME);

        // Prefer conf option over  $HOME/.pegasusrc
        File confProps = null;
        props =
                (confProperties != null && (confProps = new File(confProperties)).exists())
                        ? confProps
                        : props;

        if (props.exists()) {
            // if this file exists, read the properties (will throw IOException)
            Properties temp = new Properties();
            InputStream stream = new BufferedInputStream(new FileInputStream(props));
            // PM-1593 load everything in properties as UTF-8
            temp.load(new InputStreamReader(stream, DEFAULT_ENCODING_SET));
            stream.close();

            this.m_props = addProperties(this.m_props, temp);
        }

        // now set the paths: set sysconfdir to correct latest value
        this.m_binDir =
                pickPath(
                        this.m_props.getProperty("pegasus.home.bindir"),
                        System.getProperty("pegasus.home.bindir"));
        this.m_sysConfDir =
                pickPath(
                        this.m_props.getProperty("pegasus.home.sysconfdir"),
                        System.getProperty("pegasus.home.sysconfdir"));
        this.m_sharedStateDir =
                pickPath(
                        this.m_props.getProperty("pegasus.home.sharedstatedir"),
                        System.getProperty("pegasus.home.sharedstatedir"));
        this.m_schemaDir =
                pickPath(
                        this.m_props.getProperty("pegasus.home.schemadir"),
                        System.getProperty("pegasus.home.schemadir"));
    }

    private File pickPath(String p1, String p2) {
        String winner = null;
        if (p1 != null) {
            winner = p1;
        } else if (p2 != null) {
            winner = p2;
        }

        if (winner != null) {
            return new File(winner);
        }
        return null;
    }

    /**
     * Singleton threading: Creates the one and only instance of the properties in the current
     * application.
     *
     * @return a reference to the properties.
     * @exception IOException will be thrown if reading the property file goes awry.
     * @exception MissingResourceException will be thrown if you forgot to specify the <code>
     *     -Dpegasus.home=$PEGASUS_HOME</code> to the runtime environment.
     * @see #noHassleInstance()
     */
    public static CommonProperties instance() throws IOException, MissingResourceException {
        if (CommonProperties.m_instance == null)
            CommonProperties.m_instance = new CommonProperties(null);
        return CommonProperties.m_instance;
    }

    /**
     * Create a temporary property that is not attached to the Singleton. This may be helpful with
     * portal, which do magic things during the lifetime of a process.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     * @return a reference to the parsed properties.
     * @exception IOException will be thrown if reading the property file goes awry.
     * @exception MissingResourceException will be thrown if you forgot to specify the <code>
     *     -Dpegasus.home=$PEGASUS_HOME</code> to the runtime environment.
     * @see #instance()
     */
    public static CommonProperties nonSingletonInstance(String confProperties)
            throws IOException, MissingResourceException {
        return new CommonProperties(confProperties);
    }

    /**
     * Singleton interface: Creates the one and only instance of the properties in the current
     * application, and does not bother the programmer with exceptions. Rather, exceptions from the
     * underlying <code>instance()</code> call are caught, converted to an error message on stderr,
     * and the program is exited.
     *
     * @return a reference to the properties.
     * @see #instance()
     */
    public static CommonProperties noHassleInstance() {
        CommonProperties result = null;
        try {
            result = instance();
        } catch (IOException e) {
            System.err.println("While reading property file: " + e.getMessage());
            System.exit(1);
        } catch (MissingResourceException mre) {
            System.err.println(mre.getMessage());
            System.exit(1);
        }
        return result;
    }

    /**
     * Accessor pegasus bin directory
     *
     * @return the "bin" directory of the Pegasus runtime system.
     */
    public File getBinDir() {
        return this.m_binDir;
    }

    /**
     * Accessor to $PEGASUS_HOME/etc. The files in this directory have a low change frequency, are
     * effectively read-only, they reside on a per-machine basis, and they are valid usually for a
     * single user.
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSysConfDir() {
        return this.m_sysConfDir;
    }

    /**
     * Accessor to $PEGASUS_HOME/com. The files in this directory have a high change frequency, are
     * effectively read-write, they reside on a per-machine basis, and they are valid usually for a
     * single user.
     *
     * @return the "com" directory of the VDS runtime system.
     */
    public File getSharedStateDir() {
        return this.m_sharedStateDir;
    }

    /**
     * Accessor to schema directory
     *
     * @return the schema directoru
     */
    public File getSchemaDir() {
        return this.m_schemaDir;
    }

    /**
     * Accessor: Obtains the number of properties known to the project.
     *
     * @return number of properties in the project property space.
     */
    public int size() {
        return this.m_props.size();
    }

    /**
     * Accessor: access to the internal properties as read from file. An existing system property of
     * the same key will have precedence over any project property. This method will remove leading
     * and trailing ASCII control characters and whitespaces.
     *
     * @param key is the key to look up
     * @return the value for the key, or null, if not found.
     */
    public String getProperty(String key) {
        String result = System.getProperty(key, this.m_props.getProperty(key));
        return (result == null ? result : result.trim());
    }

    /**
     * Accessor: access to the internal properties as read from file An existing system property of
     * the same key will have precedence over any project property. This method will remove leading
     * and trailing ASCII control characters and whitespaces.
     *
     * @param key is the key to look up
     * @param defValue is a default to use, if no value can be found for the key.
     * @return the value for the key, or the default value, if not found.
     */
    public String getProperty(String key, String defValue) {
        String result = System.getProperty(key, this.m_props.getProperty(key, defValue));
        return (result == null ? result : result.trim());
    }

    /**
     * Accessor: Overwrite any properties from within the program.
     *
     * @param key is the key to look up
     * @param value is the new property value to place in the system.
     * @return the old value, or null if it didn't exist before.
     */
    public Object setProperty(String key, String value) {
        // set in internal properties object also
        // else prefix option does not work. Karan Oct 1, 2008
        return this.m_props.setProperty(key, value);
        // we don't set System properties, else it makes the clone method
        // unusable
        // return System.setProperty( key, value );
    }

    /**
     * Accessor: enumerate all keys known to this property collection
     *
     * @return an enumerator for the keys of the properties.
     */
    public Enumeration propertyNames() {
        return this.m_props.propertyNames();
    }

    /**
     * Extracts a specific property key subset from the known properties. The prefix may be removed
     * from the keys in the resulting dictionary, or it may be kept. In the latter case, exact
     * matches on the prefix will also be copied into the resulting dictionary.
     *
     * @param prefix is the key prefix to filter the properties by.
     * @param keepPrefix if true, the key prefix is kept in the resulting dictionary. As
     *     side-effect, a key that matches the prefix exactly will also be copied. If false, the
     *     resulting dictionary's keys are shortened by the prefix. An exact prefix match will not
     *     be copied, as it would result in an empty string key.
     * @return a property dictionary matching the filter key. May be an empty dictionary, if no
     *     prefix matches were found.
     * @see #getProperty( String ) is used to assemble matches
     */
    public Properties matchingSubset(String prefix, boolean keepPrefix) {
        Properties result = new Properties();

        // sanity check
        if (prefix == null || prefix.length() == 0) return result;

        String prefixMatch; // match prefix strings with this
        String prefixSelf; // match self with this
        if (prefix.charAt(prefix.length() - 1) != '.') {
            // prefix does not end in a dot
            prefixSelf = prefix;
            prefixMatch = prefix + '.';
        } else {
            // prefix does end in one dot, remove for exact matches
            prefixSelf = prefix.substring(0, prefix.length() - 1);
            prefixMatch = prefix;
        }
        // POSTCONDITION: prefixMatch and prefixSelf are initialized!

        // now add all matches into the resulting properties.
        // Remark 1: #propertyNames() will contain the System properties!
        // Remark 2: We need to give priority to System properties. This is done
        // automatically by calling this class's getProperty method.
        String key;

        List<Enumeration> enumerations = new LinkedList();

        // take cares of only the properties specified in the properties file
        enumerations.add(propertyNames());
        // user may have specified profiles as properties in the system
        // fix for PM-581
        enumerations.add(System.getProperties().propertyNames());

        for (Enumeration e : enumerations) {
            while (e.hasMoreElements()) {
                key = (String) e.nextElement();

                if (keepPrefix) {
                    // keep full prefix in result, also copy direct matches
                    if (key.startsWith(prefixMatch) || key.equals(prefixSelf))
                        result.setProperty(key, getProperty(key));
                } else {
                    // remove full prefix in result, dont copy direct matches
                    if (key.startsWith(prefixMatch))
                        result.setProperty(key.substring(prefixMatch.length()), getProperty(key));
                }
            }
        }

        // done
        return result;
    }

    /**
     * Extracts a specific property key subset from the properties passed. The prefix may be removed
     * from the keys in the resulting dictionary, or it may be kept. In the latter case, exact
     * matches on the prefix will also be copied into the resulting dictionary.
     *
     * @param prefix is the key prefix to filter the properties by.
     * @param keepPrefix if true, the key prefix is kept in the resulting dictionary. As
     *     side-effect, a key that matches the prefix exactly will also be copied. If false, the
     *     resulting dictionary's keys are shortened by the prefix. An exact prefix match will not
     *     be copied, as it would result in an empty string key.
     * @return a property dictionary matching the filter key. May be an empty dictionary, if no
     *     prefix matches were found.
     * @see #getProperty( String ) is used to assemble matches
     */
    public static Properties matchingSubset(
            Properties properties, String prefix, boolean keepPrefix) {
        Properties result = new Properties();

        // sanity check
        if (prefix == null || prefix.length() == 0) return result;

        String prefixMatch; // match prefix strings with this
        String prefixSelf; // match self with this
        if (prefix.charAt(prefix.length() - 1) != '.') {
            // prefix does not end in a dot
            prefixSelf = prefix;
            prefixMatch = prefix + '.';
        } else {
            // prefix does end in one dot, remove for exact matches
            prefixSelf = prefix.substring(0, prefix.length() - 1);
            prefixMatch = prefix;
        }
        // POSTCONDITION: prefixMatch and prefixSelf are initialized!

        // now add all matches into the resulting properties.
        // Remark 1: #propertyNames() will contain the System properties!
        // Remark 2: We need to give priority to System properties. This is done
        // automatically by calling this class's getProperty method.
        String key;
        for (Enumeration e = properties.propertyNames(); e.hasMoreElements(); ) {
            key = (String) e.nextElement();

            if (keepPrefix) {
                // keep full prefix in result, also copy direct matches
                if (key.startsWith(prefixMatch) || key.equals(prefixSelf))
                    result.setProperty(key, properties.getProperty(key));
            } else {
                // remove full prefix in result, dont copy direct matches
                if (key.startsWith(prefixMatch))
                    result.setProperty(
                            key.substring(prefixMatch.length()), properties.getProperty(key));
            }
        }

        // done
        return result;
    }

    /**
     * Print out the property list onto the specified stream. This method is useful for debugging,
     * and meant for debugging.
     *
     * @param out an output stream
     * @throws ClassCastException if any key is not a string.
     * @see java.util.Properties#list( PrintStream )
     */
    public void list(PrintStream out) {
        m_props.list(out);
    }

    public static void main(String[] args) throws java.io.IOException {
        CommonProperties cp = null;
        if (args.length > 0) cp = CommonProperties.nonSingletonInstance(args[0]);
        else cp = CommonProperties.instance();

        cp.list(System.out);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        CommonProperties props;
        try {

            // this will do a shallow clone for all member variables
            // that is fine for the string variables
            props = (CommonProperties) super.clone();

            // do a deep clone for the underlying properties
            // can be expensive
            props.m_props = new Properties();
            for (Object key : this.m_props.keySet()) {
                props.m_props.put(key, this.m_props.get(key));
            }
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return props;
    }

    /**
     * Removes a property from the soft state.
     *
     * @param key the key
     * @return the corresponding value if key exits, else null
     */
    public String removeProperty(String key) {
        return (String) this.m_props.remove(key);
    }
}
