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
package edu.isi.pegasus.planner.catalog.replica.impl;

import edu.isi.pegasus.common.util.Escape;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This class implements a replica catalog which directly writes to the output file. This is done so
 * that the Cache file generation would not require storing all RC entries in memory.
 *
 * <p>The implementation does not maintain any internal state, and is not aware if same entries are
 * being written multiple times.
 *
 * <p>The class using this implementation needs to ensure only valid entries are passed through to
 * the insert method, and that entries are not being duplicated.
 *
 * @author Rajiv Mayani
 * @version $Revision: 5907 $
 */
public class FlushedCache implements ReplicaCatalog {

    /** A buffered writer, to buffer all file writes being performed. */
    protected BufferedWriter m_out;

    /** No. of entries to buffer. */
    private int m_buffer_size = 1000;

    /** No. of entries to buffer. */
    private static final int m_avg_line_size = 100;

    /** No. of un-flushed entries. */
    private int m_line_count = 0;

    /** Set buffer size i.e. The no of lines that the implementation will buffer. */
    public void setBufferSize(int bufferSize) {
        m_buffer_size = bufferSize;
    }

    /**
     * Opens the file for writing.
     *
     * @param filename is the name of the file to write too.
     * @return true, if the file can be opened for writing
     */
    public boolean connect(String filename) {
        // sanity check
        if (filename == null) {
            return false;
        }

        try {
            // Create a buffered writer with a buffer size of m_buffer_size
            // lines, with each line averaging a m_avg_line_size bytes
            m_out = new BufferedWriter(new FileWriter(filename), m_buffer_size * m_avg_line_size);
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Establishes a connection to the database from the properties. You will need to specify a
     * "file" property to point to the location of the on-disk instance.
     *
     * @param props is the property table with sufficient settings to establish a link with the
     *     database.
     * @return true if connected, false if failed to connect.
     */
    public boolean connect(Properties props) {

        if (props.containsKey("file")) {
            return connect(props.getProperty("file"));
        }

        return false;
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
            result = s;
        } else {
            // string has content
            boolean flag = false;
            for (int i = 0; i < s.length() && !flag; ++i) {
                // Note: loop will never trigger, if m_quote is true
                char ch = s.charAt(i);
                flag = (ch == '"' || ch == '\\' || ch == '=' || Character.isWhitespace(ch));
            }

            result = (flag ? '"' + e.escape(s) + '"' : s);
        }

        // single point of exit
        return result;
    }

    /**
     * The method generate a String representation of the replica catalog entry. The String
     * representation follows the format expected by SimpleFile replica catalog implementation.
     *
     * @param lfn The logical file name
     * @param rce The replica catalog entry consisting of the physical file name name and key, value
     *     pairs.
     * @return A String representation of the replica-catalog entry.
     */
    protected String writeReplicaCatalogEntry(String lfn, ReplicaCatalogEntry rce) {
        Escape e = new Escape("\"\\", '\\');
        StringBuilder s = new StringBuilder();

        s.append(quote(e, lfn));
        s.append(' ');
        s.append(quote(e, rce.getPFN()));

        for (Iterator k = rce.getAttributeIterator(); k.hasNext(); ) {
            String key = (String) k.next();
            String value = (String) rce.getAttribute(key);
            s.append(' ');
            s.append(key);
            s.append("=\"");
            s.append(e.escape(value));
            s.append('"');
        }

        m_line_count++;
        s.append(System.getProperty("line.separator", "\r\n"));
        return s.toString();
    }

    /**
     * Check if the buffer is full i.e. No. of unwritten lines is same as m_buffer_size
     *
     * @see setBufferSize
     */
    private void flush() {
        if (m_buffer_size - m_line_count == 0) {
            try {
                m_line_count = 0;
                m_out.flush();
            } catch (IOException ioe) {
                System.err.println(ioe.getMessage());
            }
        }
    }

    /**
     * This operation will dump the in-memory representation back onto disk. The store operation is
     * strict in what it produces. The LFN and PFN records are only quoted, if they require quotes,
     * because they contain special characters. The attributes are <b>always</b> quoted and thus
     * quote-escaped.
     */
    public void close() {

        try {
            if (m_out != null) {
                m_out.flush();
            }
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } finally {

            if (m_out != null) {
                try {
                    if (m_out != null) {
                        m_out.close();
                    }
                } catch (IOException ioe) {
                    System.err.println(ioe.getMessage());
                }

                m_out = null;
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
        return (m_out == null);
    }

    public String lookup(String lfn, String handle) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Collection lookup(String lfn) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Set lookupNoAttributes(String lfn) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Map lookup(Set lfns) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Map lookupNoAttributes(Set lfns) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Map lookup(Set lfns, String handle) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Map lookupNoAttributes(Set lfns, String handle) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Map lookup(Map constraints) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Set list() {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public Set list(String constraint) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
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
        if (lfn == null || tuple == null) {
            throw new NullPointerException();
        }

        write(writeReplicaCatalogEntry(lfn, tuple));

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
     * @see #insert(String, ReplicaCatalogEntry )
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        if (lfn == null || pfn == null || handle == null) {
            throw new NullPointerException();
        }

        write(writeReplicaCatalogEntry(lfn, new ReplicaCatalogEntry(pfn, handle)));
        return 1;
    }

    /**
     * Inserts multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. Note that this
     * operation will not replace existing entries.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @return the number of insertions.
     * @see org.griphyn.common.catalog.ReplicaCatalogEntry
     */
    public int insert(Map x) {
        int result = 0;

        // shortcut sanity
        if (x == null || x.isEmpty()) {
            return result;
        }

        for (Iterator i = x.keySet().iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Object val = x.get(lfn);
            if (val instanceof ReplicaCatalogEntry) {

                write(writeReplicaCatalogEntry(lfn, (ReplicaCatalogEntry) val));
            } else {
                // this is how it should have been
                for (Iterator j = ((Collection) val).iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    write(writeReplicaCatalogEntry(lfn, (ReplicaCatalogEntry) val));
                }
            }
        }

        return result;
    }

    private void write(String content) {
        try {
            m_out.write(content);
            flush();
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public int delete(String lfn, String pfn) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int delete(Map x, boolean matchAttributes) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int delete(String lfn, String name, Object value) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int deleteByResource(String lfn, String handle) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int remove(String lfn) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int remove(Set lfns) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int removeByAttribute(String name, Object value) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int removeByAttribute(String handle) {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    public int clear() {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        throw new java.lang.UnsupportedOperationException("Method not implemented");
    }

    /**
     * Set the catalog to read-only mode.
     *
     * @param readonly whether the catalog is read-only
     */
    @Override
    public void setReadOnly(boolean readonly) {
        // do nothing
    }
}
