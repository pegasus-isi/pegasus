/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.client;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.griphyn.vdl.toolkit.Toolkit;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

/**
 * This class interfaces the with the replica catalog API to delve into the
 * underlying true catalog without knowing (once instantiated) which one it is.
 * 
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * 
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalog
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
 * @see edu.isi.pegasus.planner.catalog.replica.impl.JDBCRC
 */
public class RCClient extends Toolkit {

    /**
     * The message for LFN's not found.
     */
    private static final String LFN_DOES_NOT_EXIST_MSG = "LFN doesn't exist:";

    /**
     * The default chunk factor that is used for biting off chunks of large
     * files.
     */
    private static final int DEFAULT_CHUNK_FACTOR = 1000;

    /**
     * Maintains the interface to the replica catalog implementation.
     */
    private ReplicaCatalog m_rc;

    /**
     * Maintains instance-local settings on user preferences.
     */
    private Map m_prefs;

    /**
     * Keeps track of log4j's root logger as singleton.
     */
    private static Logger m_root;

    /**
     * Logger for RLS implementation for the time being.
     */
    private LogManager m_rls_logger;

    /**
     * The number of lines that are to be parsed for chunking up large input
     * files.
     */
    private int m_chunk_factor;

    /**
     * The total number of lines on which the client has worked on till yet.
     */
    private int m_total_lines_worked;

    /**
     * The total number of lines on which the client has successfully worked on
     * till yet.
     */
    private int m_total_lines_succ_worked;

    /**
     * Indication of batch mode.
     */
    private boolean m_batch;
    
    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties m_pegasus_props;
    
    /**
     * Reference to the property file passed using the --conf option
     */
    private String m_conf_property_file = null;

    /**
     * Initializes the root logger when this class is loaded.
     */
    static {
	if ((m_root = Logger.getRootLogger()) != null) {
	    m_root.removeAllAppenders(); // clean house
	    m_root.addAppender(new ConsoleAppender(new PatternLayout(
		    "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5p [%c{1}] %m%n")));
	    m_root.setLevel(Level.INFO);
	    m_root.debug("starting");
	}
    }

    /**
     * Sets a logging level.
     * 
     * @param level
     *            is the new level to achieve.
     */
    public void setLevel(int level) {
	doSet(Level.toLevel(level));
    }

    /**
     * Logs messages from main() method.
     * 
     * @param level
     *            is the log4j level to generate the log message for
     * @param msg
     *            is the message itself.
     * 
     * @see org.apache.log4j.Category#log(Priority, Object )
     */
    public static void log(Level level, String msg) {
	m_root.log(level, msg);
    }

    /**
     * Our own logger.
     */
    private Logger m_log;

    private void doSet(Level level) {
	m_root.setLevel(level);
	m_log.setLevel(level);
	m_rls_logger.setLevel(level);
    }

    /**
     * Adds a preference to the instance preferences settings.
     * 
     * @param key
     *            is a key into the preference map.
     * @param value
     *            is the new value to add.
     * @return the previous value, or null if no such value exists.
     */
    public Object enter(String key, String value) {
	String newkey = key.toLowerCase();
	Object result = m_prefs.put(newkey, value);
	return result;
    }

    /**
     * ctor: Constructs a new instance of the commandline interface to replica
     * catalogs.
     * 
     * @param appName
     *            is the name of to print in usage records.
     */
    public RCClient(String appName) {
	super(appName);
    }
    
    /**
     * Initialize the RCClient object 
     * @param opts  the command line argument passed by the user
     * @param confChar the short option corresponding the conf property.
     */
    
    private void initialize(String [] opts , char confChar){
    	m_rc = null;
    	m_prefs = new HashMap();
    	m_batch = false;
    	m_total_lines_worked = 0;
    	m_total_lines_succ_worked = 0;
    	// private logger
    	m_log = Logger.getLogger(RCClient.class);
    	String propertyFile =lookupConfProperty(opts, confChar);
        m_pegasus_props = PegasusProperties.getInstance(propertyFile);
        m_conf_property_file = propertyFile;
    	m_rls_logger = LogManagerFactory.loadSingletonInstance(m_pegasus_props);
    	m_rls_logger.setLevel(Level.WARN);
    	m_rls_logger.logEventStart("pegasus-rc-client", "planner.version",
    		Version.instance().toString());
    	m_log.debug("starting instance");
    	determineChunkFactor();
    }
    
    
    
    /**
     * Prints the usage string on stdout.
     */
    public void showUsage() {
	String linefeed = System.getProperty("line.separator", "\r\n");
	System.out
		.println("$Id$"
			+ linefeed
			+ "Pegasus version "
			+ Version.instance().toString() + linefeed);

	System.out
		.println("Usage: "
			+ this.m_application
			+ " [-p k=v] [ [-f fn] | [-i|-d fn] | [cmd [args]] ]"
			+ linefeed
			+ " -h|--help      print this help text"
			+ linefeed
			+ " -V|--version   print some version identification string and exit"
			+ linefeed
			+ " -f|--file fn   uses non-interactive mode, reading from file fn."
			+ linefeed
			+ "                The special filename hyphen reads from pipes"
			+ linefeed
			+ " -c|--conf fn   path to the property file"
			+ linefeed
			+ " -v|--verbose   increases the verbosity level"
			+ linefeed
			+ " -p|--pref k=v  enters the specified mapping into preferences (multi-use)."
			+ linefeed
			+ "                remember quoting, e.g. -p 'format=%l %p %a'"
			+ linefeed
			+

			" -i|--insert fn the path to the file containing the mappings to be inserted."
			+ linefeed
			+ "                Each line in the file denotes one mapping of format <LFN> <PFN> [k=v [..]]"
			+ linefeed
			+ " -d|--delete fn the path to the file containing the mappings to be deleted."
			+ linefeed
			+ "                Each line in the file denotes one mapping of format <LFN> <PFN> [k=v [..]]."
			+ linefeed
			+ " -l|--lookup fn the path to the file containing the LFN's to be looked up."
			+ linefeed
			+ "                Each line in the file denotes one LFN"
			+ linefeed
			+ "                For now attributes are not matched to determine the entries to delete."
			+ linefeed
			+ " cmd [args]     exactly one of the commands below with arguments.");

	showHelp();

	System.out
		.println("FIXME list:"
			+ linefeed
			+ " o permit input to span multiple lines (format free input)"
			+ linefeed
			+ " o permit whitespaces within PFNs (but not in SITE nor LFN)"
			+ linefeed
			+ " o permit commands to deal with values that contain whitespaces (quoting)"
			+ linefeed
			+ " o add some missing out-of-bounds checks to the format string"
			+ linefeed);

    }

    /**
     * Creates a set of GNU long options.
     * 
     * @return an initialized array with the options
     */
    protected LongOpt[] generateValidOptions() {
	LongOpt[] lo = new LongOpt[9];

	lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
	lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
	lo[2] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');
	lo[3] = new LongOpt("pref", LongOpt.REQUIRED_ARGUMENT, null, 'p');
	lo[4] = new LongOpt("insert", LongOpt.REQUIRED_ARGUMENT, null, 'i');
	lo[5] = new LongOpt("delete", LongOpt.REQUIRED_ARGUMENT, null, 'd');
	lo[6] = new LongOpt("lookup", LongOpt.REQUIRED_ARGUMENT, null, 'l');
	lo[7] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
	lo[8] = new LongOpt( "conf", LongOpt.REQUIRED_ARGUMENT, null, 'c' );
	return lo;
    }

    /**
     * Connects the interface with the replica catalog implementation. The
     * choice of backend is configured through properties.
     * 
     * @exception ClassNotFoundException
     *                if the schema for the database cannot be loaded. You might
     *                want to check your CLASSPATH, too.
     * @exception NoSuchMethodException
     *                if the schema's constructor interface does not comply with
     *                the database driver API.
     * @exception InstantiationException
     *                if the schema class is an abstract class instead of a
     *                concrete implementation.
     * @exception IllegalAccessException
     *                if the constructor for the schema class it not publicly
     *                accessible to this package.
     * @exception InvocationTargetException
     *                if the constructor of the schema throws an exception while
     *                being dynamically loaded.
     * @exception IOException
     * @exception MissingResourceException
     * 
     */
    void connect(PegasusProperties properties, String file ) throws ClassNotFoundException, IOException,
	    NoSuchMethodException, InstantiationException,
	    IllegalAccessException, InvocationTargetException,
	    MissingResourceException {
  
        m_rc = ReplicaFactory.loadInstance(properties, file);

	// auto-disconnect, should we forget it, or die in an orderly fashion
	Runtime.getRuntime().addShutdownHook(new Thread() {
	    public void run() {
		try {
		    // log for the batch mode
		    if (m_batch) {
			// log on stderr to prevent clobbing
			System.err.println("#Successfully worked on    : "
				+ m_total_lines_succ_worked + " lines.");
			System.err.println("#Worked on total number of : "
				+ m_total_lines_worked + " lines.");
		    }
		    // disconnect from the replica catalog
		    close();
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	});

    }

    /**
     * Frees resources taken by the instance of the replica catalog. This method
     * is safe to be called on failed or already closed catalogs.
     */
    void close() {
	if (m_rc != null) {
	    m_rc.close();
	    m_rc = null;
	}
    }

    /**
     * Escapes quotes and backslashes by backslashing them. Identity s ==
     * unescape(escape(s)) is preserved.
     * 
     * @param s
     *            is the string to escape
     * @return a string with escaped special characters.
     * @see #unescape(String )
     */
    private String escape(String s) {
	StringBuffer result = new StringBuffer(s.length());

	for (int i = 0; i < s.length(); ++i) {
	    char ch = s.charAt(i);
	    if (ch == '"' || ch == '\\')
		result.append('\\');
	    result.append(ch);
	}

	return result.toString();
    }

    /**
     * Unescapes previously backslashed characters. Identity s ==
     * unescape(escape(s)) is preserved.
     * 
     * @param s
     *            is the string to escape
     * @return a string with unescaped special characters.
     * @see #escape(String )
     */
    private String unescape(String s) {
	StringBuffer result = new StringBuffer(s.length());
	int state = 0;

	for (int i = 0; i < s.length(); ++i) {
	    char ch = s.charAt(i);
	    if (state == 0) {
		if (ch == '\\')
		    state = 1;
		else
		    result.append(ch);
	    } else {
		result.append(ch);
		state = 0;
	    }
	}

	return result.toString();
    }

    /**
     * Removes a pair of outer quotes, which are optional.
     * 
     * @param s
     *            is a string which may start and end in quotes
     * @return a string without the optional quotes, or the string itself.
     */
    private String noquote(String s) {
	int len = s.length();

	// remove outer quotes, if they exist
	return ((s.charAt(0) == '"' && s.charAt(len - 1) == '"') ? s.substring(
		1, len - 1) : s);
    }

    /**
     * Writes out a message about LFN not existing.
     * 
     * @param lfn
     *            the lfn.
     */
    private void lfnDoesNotExist(String lfn) {
	System.err.println(LFN_DOES_NOT_EXIST_MSG + " " + lfn);
    }

    /**
     * Preliminary implementation of output method.
     * 
     * @param lfn
     *            is the logical filename to show
     * @param rce
     *            is the replica catalog entry to show. It contains at minimum
     *            the physical filename, and may contain any number of key-value
     *            pairs.
     */
    private void show(String lfn, ReplicaCatalogEntry rce) {
	System.out.print(lfn + " " + rce.getPFN());
	for (Iterator i = rce.getAttributeIterator(); i.hasNext();) {
	    String key = (String) i.next();
	    Object val = rce.getAttribute(key);
	    if (val == null)
		    continue;
	    System.out.print(" " + key + "=\"" + escape(val.toString()) + "\"");
	}
	System.out.println();
    }

    /**
     * Prints internal command help.
     */
    public void showHelp() {
	String linefeed = System.getProperty("line.separator", "\r\n");
	System.out.println(linefeed
		+ "Commands and their respective arguments, line-by-line:"
		+ linefeed + " help" + linefeed + " quit" + linefeed + " exit"
		+ linefeed + " clear" + linefeed + " insert LFN PFN [k=v [..]]"
		+ linefeed + " delete LFN PFN [k=v [..]]" + linefeed
		+ " remove LFN [LFN [..]]" + linefeed
		+ " lookup LFN [LFN [..]]" + linefeed
		+ " list   [lfn <pattern>] [pfn <pattern>] [<name> <pattern>]"
		+ linefeed + " set    [var [value]]" + linefeed);
    }

    /**
     * Works on the command contained within chunk of lines.
     * 
     * @param lines
     *            is a list of lines with each line being a list of words that
     *            is split appropriately
     * @param command
     *            the command to be invoked.
     * 
     * @return number of entries affected, or -1 to stop processing.
     */
    public int work(List lines, String command) {
	// sanity checks
	if (command == null)
	    throw new RuntimeException(
		    "The command to be applied to the file contents not specified");

	if (lines == null || lines.isEmpty())
	    return 0;

	String c_argnum = "Illegal number of arguments, ignoring!";
	int result = 0;
	// a map indexed by lfn
	Map entries = new HashMap();
	if (command.equals("insert") || command.equals("delete")) {
	    for (Iterator it = lines.iterator(); it.hasNext();) {
		List words = (List) it.next();
		if (words.size() < 2) {
		    m_log.warn(c_argnum);
		} else {
		    Iterator i = words.listIterator();
		    String lfn = (String) i.next();
		    ReplicaCatalogEntry rce = new ReplicaCatalogEntry(
			    noquote((String) i.next()));

		    while (i.hasNext()) {
			String attr = (String) i.next();
			int pos = attr.indexOf('=');
			if (pos == -1) {
			    m_log.error("attribute \"" + attr
				    + "\" without assignment, "
				    + "assuming resource handle");
			    rce.setResourceHandle(attr);
			} else {
			    rce.setAttribute(attr.substring(0, pos),
				    unescape(noquote(attr.substring(pos + 1))));
			}
		    }
                    rce.checkAndUpdateForPoolAttribute();
		    // check to see if the lfn is already there
		    // not doing a contains check as most of
		    // the times lfn is expected to be unique
		    // add all the old pfn's to the existing collection
		    Collection c = new ArrayList(1);
		    c.add(rce);
		    Object old = entries.put(lfn, c);
		    if (old != null)
			c.addAll((Collection) old);
		}
	    }// end of iteration over the lines
	    if (command.equals("insert")) {
		result = m_rc.insert(entries);
		m_log.info("inserted " + result + " entries");
	    } else {
		result = m_rc.delete(entries, true);
		m_log.info("deleted " + result + " entries");
	    }

	} else if (command.equals("lookup")) {

	    Set<String> lfns = new HashSet();
	    // each line has a single LFN
	    for (Iterator it = lines.iterator(); it.hasNext();) {
		List<String> words = (List) it.next();
		if (words.size() != 1) {
		    m_log.warn(c_argnum);
		}
		String lfn = words.get(0);
		lfns.add(lfn);
	    }
	    Map<String, Collection<ReplicaCatalogEntry>> results = m_rc
		    .lookup(lfns);
	    result = results.size();

	    // display results for LFN
	    for (Iterator<Map.Entry<String, Collection<ReplicaCatalogEntry>>> it = results
		    .entrySet().iterator(); it.hasNext();) {
		Map.Entry<String, Collection<ReplicaCatalogEntry>> entry = it
			.next();
		String lfn = entry.getKey();
		Collection rces = entry.getValue();

		for (Iterator j = rces.iterator(); j.hasNext();) {
		    show(lfn, (ReplicaCatalogEntry) j.next());
		}
	    }

	    // try and figure out LFN's for which mappings were not found
	    // and display them
	    lfns.removeAll(results.keySet());
	    for (String lfn : lfns) {
		lfnDoesNotExist(lfn);
	    }
	}
	return result;
    }

    /**
     * Works on the command contained within one line.
     * 
     * @param words
     *            is a list of the arguments, split appropriately
     * @return number of entries affected, or -1 to stop processing.
     */
    public int work(List words) {
	String c_argnum = "Illegal number of arguments, ignoring!";
	int result = 0;

	// sanity check
	if (words == null || words.size() == 0)
            return result;

	// separate command from arguments
	String cmd = ((String) words.remove(0)).toLowerCase();

	if (cmd.equals("help")) {
	    showHelp();
	} else if (cmd.equals("lookup")) {
            m_rc.setReadOnly(true);
	    for (Iterator i = words.iterator(); i.hasNext();) {
		String lfn = (String) i.next();
		Collection c = m_rc.lookup(lfn);
		m_log.info("found " + c.size() + " matches");

		for (Iterator j = c.iterator(); j.hasNext();) {
		    show(lfn, (ReplicaCatalogEntry) j.next());
		    result++;
		}
	    }
	} else if (cmd.equals("list")) {
            m_rc.setReadOnly(true);
	    Map m = new HashMap();
	    for (Iterator i = words.iterator(); i.hasNext();) {
		String key = ((String) i.next()).toLowerCase();
		if (i.hasNext()) {
		    String val = (String) i.next();
		    m.put(key, val);
		}
	    }

	    Map lfns = m_rc.lookup(m);
	    if (lfns.size() > 0) {
		for (Iterator i = lfns.keySet().iterator(); i.hasNext();) {
		    String lfn = (String) i.next();
		    for (Iterator j = ((List) lfns.get(lfn)).iterator(); j
			    .hasNext();) {
			show(lfn, (ReplicaCatalogEntry) j.next());
			result++;
		    }
		}
		m_log.info("found " + result + " matches");
	    } else {
		m_log.info("no matches found");
	    }
	} else if (cmd.equals("insert") || cmd.equals("delete")) {
	    if (words.size() < 2) {
		m_log.warn(c_argnum);
	    } else {
		Iterator i = words.listIterator();
		String lfn = (String) i.next();
		ReplicaCatalogEntry rce = new ReplicaCatalogEntry(
			noquote( (String) i.next() ) );

		while (i.hasNext()) {
		    String attr = (String) i.next();
		    int pos = attr.indexOf('=');
		    if (pos == -1) {
			m_log.error("attribute \"" + attr
				+ "\" without assignment, "
				+ "assuming resource handle");
			rce.setResourceHandle(attr);
		    } else {
			rce.setAttribute(attr.substring(0, pos),
				unescape(noquote(attr.substring(pos + 1))));
		    }
		}
                //PM-813 backward support for pool attribute
                rce.checkAndUpdateForPoolAttribute();

		if (cmd.equals("insert")) {
		    result = m_rc.insert(lfn, rce);
		    m_log.info("inserted " + result + " entries");
		} else {
		    result = rce.getAttributeCount() == 0 ? m_rc.delete(lfn,
			    rce.getPFN()) : m_rc.delete(lfn, rce);
		    m_log.info("deleted " + result + " entries");
		}
	    }
	} else if (cmd.equals("remove")) {
	    // do it the slow way, better debugging
	    for (Iterator i = words.iterator(); i.hasNext();) {
		String lfn = (String) i.next();
		int count = m_rc.remove(lfn);
		result += count;
		if (count > 0) {
		    m_log.info("removed LFN " + lfn);
		} else {
		    m_log.info("ignoring unknown LFN " + lfn);
		}
	    }
	} else if (cmd.equals("clear")) {
	    result = m_rc.clear();
	    m_log.info("removed " + result + " entries");
	} else if (cmd.equals("quit") || cmd.equals("exit")) {
	    result = -1;
	    m_log.info("Good-bye");
	} else if (cmd.equals("set")) {
	    String key, value;
	    switch (words.size()) {
	    case 0: // show all
		for (Iterator i = m_prefs.keySet().iterator(); i.hasNext();) {
		    key = (String) i.next();
		    value = (String) m_prefs.get(key);
		    System.out.println("set " + key + " " + value);
		    result++;
		}
		break;
	    case 1: // show one
		key = ((String) words.get(0)).toLowerCase();
		if (m_prefs.containsKey(key)) {
		    value = (String) m_prefs.get(key);
		    System.out.println("set " + key + " " + value);
		    result++;
		} else {
		    m_log.warn("no such preference");
		}
		break;
	    case 2: // set one
		enter((String) words.get(0), (String) words.get(1));
		result++;
		break;
	    default: // other
		m_log.warn(c_argnum);
		break;
	    }
	} else {
	    // unknown command
	    m_log.warn("Unknown command: " + cmd + ", ignoring!");
	}

	return result;
    }

    /**
     * Consumes commands that control the replica management.
     * 
     * @param filename
     *            is the file to read from. If null, use stdin.
     * @exception IOException
     */
    public void parse(String filename) throws IOException {
	boolean prompt = (filename == null);

	LineNumberReader lnr = null;
	if (filename != null) {
	    // connect to file, use non-interactive mode
	    if (filename.equals("-"))
		// reading from a pipe, don't prompt
		lnr = new LineNumberReader(new InputStreamReader(System.in));
	    else
		// reading from a file, don't prompt
		lnr = new LineNumberReader(new FileReader(filename));
	} else {
	    // connect to stdin
	    lnr = new LineNumberReader(new InputStreamReader(System.in));
	}

	int pos, result = 0;
	String line;
	StringTokenizer st;
	List words = new ArrayList();

	if (prompt)
	    System.out.print("rc> ");
	while ((line = lnr.readLine()) != null) {
	    // do away with superflous whitespaces and comments
	    if ((pos = line.indexOf('#')) != -1)
		line = line.substring(0, pos);
	    line = line.trim();

	    // skip empty lines
	    if (line.length() == 0)
		continue;

	    // repeat what we are working on now
	    m_log.debug("LINE " + lnr.getLineNumber() + ": " + line);
	    words.clear();
	    st = new StringTokenizer(line);
	    while (st.hasMoreTokens())
		words.add(st.nextToken());
	    try {
		if (work(words) == -1)
		    break;
	    } catch (ReplicaCatalogException rce) {
		do {
		    RCClient.log(Level.ERROR, rce.getMessage());
		    rce = (ReplicaCatalogException) rce.getNextException();
		} while (rce != null);
		result = 1;
	    } catch (RuntimeException rte) {
		do {
		    RCClient.log(Level.ERROR,
			    rte.getClass() + " " + rte.getMessage());
		    rte = (RuntimeException) rte.getCause();
		} while (rte != null);
		result = 1;
	    }
	    if (prompt)
		System.out.print("rc> ");
	}

	// done
	if (prompt && line == null)
	    System.out.println();
	lnr.close();

	// telmi, if something went wrong
	if (result == 1)
	    throw new RuntimeException("Errors while processing input file");
    }

    /**
     * Consumes commands that control the replica management.
     * 
     * @param filename
     *            is the file to read from.
     * @param command
     *            is the command that needs to be applied to the file contents
     * 
     * @exception IOException
     */
    public void parse(String filename, String command) throws IOException {
	LineNumberReader lnr = null;
	int chunk = m_chunk_factor;
	int lines_succ_worked = 0;

	if (command == null) {
	    // throw an exception
	    throw new RuntimeException(
		    "The command to be applied to the file contents not specified");
	}

	if (filename != null) {
	    // connect to file, use non-interactive mode
	    // reading from a file
	    lnr = new LineNumberReader(new FileReader(filename));
	} else {
	    // throw an exception
	    throw new RuntimeException(
		    "File containing the mappings not specified");
	}

	int pos, result = 0;
	String line = null;
	StringTokenizer st;
	List words;

	// set the batch mode to true
	m_batch = true;

	// contains the number of valid lines read so far in the current block
	int counter = 0;
	List mappings = new ArrayList(chunk);

	while (true) {
	    while (counter < chunk && (line = lnr.readLine()) != null) {
		// do away with superflous whitespaces and comments
		if ((pos = line.indexOf('#')) != -1)
		    line = line.substring(0, pos);
		line = line.trim();

		// skip empty lines
		if (line.length() == 0)
		    continue;

		// repeat what we are working on now
		m_total_lines_worked = lnr.getLineNumber();
		m_log.debug("LINE " + m_total_lines_worked + ": " + line);
		words = new ArrayList(chunk);
		st = new StringTokenizer(line);
		while (st.hasMoreTokens())
		    words.add(st.nextToken());

		// add to the mappings
		counter++;
		mappings.add(words);
	    }

	    // hand off the mappings for work
	    try {
		lines_succ_worked = work(mappings, command);
		m_total_lines_succ_worked += lines_succ_worked;
	    } catch (ReplicaCatalogException rce) {
		do {
		    RCClient.log(Level.ERROR, rce.getMessage());
		    rce = (ReplicaCatalogException) rce.getNextException();
		} while (rce != null);
		result = 1;
	    } catch (RuntimeException rte) {
		RCClient.log(Level.ERROR, rte.getMessage());
		result = 1;
	    } finally {
		// log the number of lines successfully worked
		m_log.info("Successfully worked on "
			+ m_total_lines_succ_worked + " lines.");
		mappings.clear();
	    }
	    m_log.info("Worked till line " + m_total_lines_worked);
	    // System.out.println();

	    // get out of the loop if end
	    if (line == null)
		break;
	    else
		counter = 0;
	}

	// done
	lnr.close();

	// telmi, if something went wrong
	if (result == 1)
	    throw new RuntimeException("Errors while processing input file");
    }
    
    
    /**
     * Looks up for the conf property in the command line arguments passed to the RCClient
     * @param opts command line arguments 
     * @param confChar short char corresponding to the conf property
     * @return path to the property file
     */
    private String lookupConfProperty(String[] opts , char confChar){
    	LongOpt[] longOptions = new LongOpt[1 ];
    	longOptions[ 0 ] = new LongOpt( "conf", LongOpt.REQUIRED_ARGUMENT, null,confChar );
    	// Create a clone before passing it to the GetOpts
    	// Getopts changes the ordering of the array. 
    	String[] optsClone = new String[opts.length];
		for(int i =0; i< opts.length;i++){
			optsClone[i] = opts[i];
		}
    	
    	Getopt g = new Getopt("RCClient", optsClone, confChar+":", longOptions, false);
		g.setOpterr(false);
    	String propertyFilePath = null;
    	int option = 0;
    	while ( ( option = g.getopt() ) != -1 ) {
    		if(option == confChar){
    			propertyFilePath = g.getOptarg();
    			break;
    		}
    	}
    	return propertyFilePath;
    }
    
    /**
     * Manipulate entries in a given replica catalog implementation.
     * 
     * @param args
     *            are the commandline arguments.
     */
    public static void main(String[] args) {
	int result = 0;
	int level = Level.ERROR_INT;
	RCClient me = null;
       
	try {
	    // create an instance of self
	    me = new RCClient("pegasus-rc-client");
	    me.initialize(args ,'c');
	    if (args.length == 0) {
		me.m_log.error("Please provide the required options.");
		me.showUsage();
		System.exit(1);
	    }
	    // get the command line options
	    Getopt opts = new Getopt(me.m_application, args, "f:hp:vVi:d:l:c:",
		    me.generateValidOptions());
	    opts.setOpterr(false);

	    String arg;
	    String filename = null;
	    int pos, option = -1;
	    boolean interactive = false;
	    String command = null;
	    while ((option = opts.getopt()) != -1) {
		switch (option) {
		case 'V':
		    System.out
			    .println("$Id$");
		    System.out.println("Pegasus version "
			    + Version.instance().toString());
		    return;
		case 'v':
		    level -= 10000;
		    break;
		case 'f':
		    arg = opts.getOptarg();
		    interactive = true;
		    if (arg != null)
			filename = arg;
		    break;
		case 'p':
		    arg = opts.getOptarg();
		    if (arg != null && (pos = arg.indexOf('=')) != -1)
			me.enter(arg.substring(0, pos), arg.substring(pos + 1));
		    break;
		case 'i':
		    arg = opts.getOptarg();
		    command = "insert";
		    if (arg != null)
			filename = arg;
		    break;
		case 'd':
		    arg = opts.getOptarg();
		    command = "delete";
		    if (arg != null)
			filename = arg;
		    break;
		case 'l':
		    arg = opts.getOptarg();
		    command = "lookup";
		    if (arg != null)
			filename = arg;
		    break;
		case 'c': // conf
			// do nothing
			break;
		case 'h':
		default:
		    me.showUsage();
		    return;
		}
	    }

	    // Set verbosity level
	    me.setLevel(level);
            // now work with me
	    me.connect(me.m_pegasus_props, me.m_conf_property_file );
	    RCClient.log(Level.DEBUG, "connected to backend");
	    // are there any remaining CLI arguments?
	    if (opts.getOptind() < args.length) {
		// there are CLI arguments
		if (filename != null) {
		    // you must not use -f and CLI extra args
		    throw new RuntimeException(
			    "The -f|-i|-d|-l option and CLI arguments "
				    + "are mutually exclusive");
		} else {
		    // just work on one (virtual, already shell-spit) line
		    List words = new ArrayList();
		    for (int i = opts.getOptind(); i < args.length; ++i)
			words.add(args[i]);
		    me.work(words);
		    RCClient.log(Level.DEBUG, "done with CLI commands");
		}
	    } else {
		// no CLI args, use single command or interactive mode
		if (interactive && command != null) {
		    throw new RuntimeException(
			    "The -f and -i|-d|-l options are mutually exclusive");
		}
		// in interactive mode parse each line
		if (interactive)
		    me.parse(filename);
		// in the command mode parse chunks of lines together
		else if (command != null)
		    me.parse(filename, command);

		RCClient.log(Level.DEBUG, "done parsing commands");
	    }

	} catch (ReplicaCatalogException rce) {
	    do {
		RCClient.log(Level.FATAL, rce.getMessage());
		rce = (ReplicaCatalogException) rce.getNextException();
	    } while (rce != null);
	    result = 1;
	} catch (RuntimeException rte) {
            Exception org = rte;
	    do {
		RCClient.log(Level.FATAL,
			rte.getClass() + " " + rte.getMessage());
		rte = (RuntimeException) rte.getCause();
	    } while (rte != null);
            
            //print stack trace if debug or higher
            //or logmanger is not set at all
            if( me == null || me.m_log == null || me.m_log.getLevel().toInt() <= Level.DEBUG_INT ){
                org.printStackTrace();
            }
            
	    result = 1;
	} catch (Exception e) {
	    RCClient.log(Level.FATAL, e.getMessage());
	    e.printStackTrace();
	    result = 2;
	} finally {
	    me.close();
	    RCClient.log(Level.DEBUG, "disconnected from backend");
	}

	// log event completion in rls logger
	me.m_rls_logger.logEventCompletion();

	// get out
	if (result != 0) {
	    RCClient.log(Level.ERROR, "non-zero exit-code " + result);
	    System.exit(result);
	}
    }

    /**
     * Sets the chunk factor for chunking up large input files.
     * 
     */
    private void determineChunkFactor() {
	int size = RCClient.DEFAULT_CHUNK_FACTOR;

	try {
	    Properties properties = CommonProperties.instance().matchingSubset(
		    ReplicaCatalog.c_prefix, false);
	    String s = properties.getProperty(ReplicaCatalog.BATCH_KEY);
	    size = Integer.parseInt(s);
	} catch (Exception e) {
	}

	m_chunk_factor = size;
    }
}
