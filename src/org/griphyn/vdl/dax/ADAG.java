/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.vdl.dax;

import edu.isi.pegasus.common.util.Currently;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import org.griphyn.vdl.classes.LFN;

/**
 * This class is the container for an abstract DAG description. It consists of three parts.
 *
 * <p>
 *
 * <ol>
 *   <li>{@link Filename} deals with the filenames that are used in the picture of the DAG - does a
 *       file go into the DAG, come out of the DAG, or is it an intermediary file. There are
 *       multiple instances stored in a DAX.
 *   <li>{@link Job} deals with the description of all jobs in a DAG. Each job has a logical
 *       transformation, commandline argument, possible stdio redirection, and a potential set of
 *       <code>Profile</code> settings. There are multiple instance stored in a DAX.
 *   <li>{@link Child} deals with the dependency in a two-level fashion. It contains a list of child
 *       to parent(s) relationships. The children and parents refer to jobs from the previous
 *       section. There are multiple instances stored in a DAX.
 * </ol>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class ADAG extends DAX implements Cloneable {
    /** The "official" namespace URI of the DAX schema. */
    public static final String SCHEMA_NAMESPACE = "https://pegasus.isi.edu/schema/DAX";

    /** The "not-so-official" location URL of the DAX schema definition. */
    public static final String SCHEMA_LOCATION = "https://pegasus.isi.edu/schema/dax-2.1.xsd";

    /** The version to report. */
    public static final String SCHEMA_VERSION = "2.1";

    /**
     * list of all filenames in terms of Filename
     *
     * @see Filename
     */
    private TreeMap m_fileMap;

    /**
     * list of all jobs
     *
     * @see Job
     */
    private TreeMap m_jobMap;

    /**
     * list of all child nodes to construct DAG.
     *
     * @see Child
     */
    private TreeMap m_childMap;

    /** list of replacements for node collapsion from compounds. */
    private TreeMap m_replace;

    private boolean m_dirty;

    /** optional name of this document. */
    private String m_name = null;

    /** When generating alternatives, this is the total number of alternatives. */
    private int m_size;

    /** When generating alternatives, this is the zero-based count. */
    private int m_index;

    /**
     * The version to report back, or alternatively to the version that the DAX file had when
     * reading.
     */
    private String m_version = SCHEMA_VERSION;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance, deep copy of elements
     */
    public Object clone() {
        ADAG result = new ADAG(this.m_size, this.m_index, this.m_name);

        // maybe unsafe?
        result.setVersion(this.m_version);

        for (Iterator i = this.m_fileMap.values().iterator(); i.hasNext(); ) {
            result.addFilename((Filename) ((Filename) i.next()).clone());
        }
        for (Iterator i = this.m_jobMap.values().iterator(); i.hasNext(); ) {
            result.addJob((Job) ((Job) i.next()).clone());
        }
        for (Iterator i = this.m_childMap.values().iterator(); i.hasNext(); ) {
            result.addChild((Child) ((Child) i.next()).clone());
        }
        for (Iterator i = this.m_replace.keySet().iterator(); i.hasNext(); ) {
            String key = (String) i.next();
            result.replaceParent(key, (String) this.m_replace.get(key));
        }

        return result;
    }

    /** Default ctor: construct a hollow shell to add data later. */
    public ADAG() {
        m_size = 1;
        m_index = 0;
        m_jobMap = new TreeMap();
        m_fileMap = new TreeMap();
        m_childMap = new TreeMap();
        m_replace = new TreeMap();
    }

    /**
     * Ctor: Construct a hollow shell with the required arguments.
     *
     * @param size is the total number of DAXes that will be constructed.
     * @param index is the zero-based number in the total number of DAXes.
     */
    public ADAG(int size, int index) {
        m_size = size;
        m_index = index;
        m_jobMap = new TreeMap();
        m_fileMap = new TreeMap();
        m_childMap = new TreeMap();
        m_replace = new TreeMap();
    }

    /**
     * Ctor: Construct a hollow shell with all element attributes
     *
     * @param size is the total number of DAXes that will be constructed.
     * @param index is the zero-based number in the total number of DAXes.
     * @param name is an optional name to use for the DAX. In later versions this might be useful,
     *     if several DAXes are interleaved on the same connection.
     */
    public ADAG(int size, int index, String name) {
        m_name = name;
        m_size = size;
        m_index = index;
        m_jobMap = new TreeMap();
        m_fileMap = new TreeMap();
        m_childMap = new TreeMap();
        m_replace = new TreeMap();
    }

    /**
     * Adds a logical filename string with input or output notion to the list of maintained
     * filenames. If the filename does not exist previously, a new entry is added. If the filename
     * does exist, the io state will be checked. A filename that was previously an input, and is now
     * an output, will become inout. If a filename was ever declared not-transfer or not-register,
     * it will maintain these attributes. Each filename is only added once.
     *
     * @param lfn is the logical filename string
     * @param isInput is a predicate with true to signal an input filename.
     * @param temporary is a temp file hint, currently unused.
     * @param dontRegister a true value will be propagated (mono-flop)
     * @param dontTransfer any non-mandatory value will be propagated
     */
    public void addFilename(
            String lfn, boolean isInput, String temporary, boolean dontRegister, int dontTransfer) {
        Filename f = (Filename) this.m_fileMap.get(lfn);
        if (f != null) {
            // found! check link status
            if ((f.getLink() == LFN.INPUT && !isInput) || (f.getLink() == LFN.OUTPUT && isInput)) {
                // need to change linkage
                f.setLink(LFN.INOUT);
            }
            // set file hint
            if (temporary != null) f.setTemporary(temporary);
            if (dontRegister) f.setRegister(!dontRegister);
            if (dontTransfer != LFN.XFER_MANDATORY) f.setTransfer(dontTransfer);
        } else {
            // file is not in list, add it
            // PS: and it is most likely not a stdio filename?
            this.m_fileMap.put(
                    lfn,
                    new Filename(
                            lfn,
                            isInput ? LFN.INPUT : LFN.OUTPUT,
                            temporary,
                            dontRegister,
                            dontTransfer,
                            null));
        }
    }

    /**
     * Adds a completely constructed {@link Filename} structure to the map of filenames. The
     * structure must be assembled outside. This method is primarily a convenience for the {@link
     * #clone()} method.
     *
     * @param lfn is the Filename instance.
     * @return true, if the bag did not contain an identical Filename already.
     */
    protected boolean addFilename(Filename lfn) {
        String id = lfn.getFilename();
        boolean result = !this.m_fileMap.containsKey(id);
        this.m_fileMap.put(id, lfn);
        return result;
    }

    /**
     * Adds a completely constructed {@link Job} structure to the map of jobs. The structure must be
     * assembled outside, using the related classes. The job ID will be taken as unique key. If a
     * job with this ID already exists in the DAX, it will be replaced with the new job.
     *
     * @param job is the new job to add
     * @return true, if the bag did not contain this job already.
     */
    public boolean addJob(Job job) {
        String id = job.getID();
        boolean result = !this.m_jobMap.containsKey(id);
        this.m_jobMap.put(id, job);
        return result;
    }

    /**
     * Adds a child node which was constructed elsewhere to the list of known children. If the child
     * already exists, nothing is done.
     *
     * @param child is the new {@link Child} instance to put into the bag.
     * @return true if the bag did not already contain the specified element.
     */
    public boolean addChild(Child child) {
        if (this.m_childMap.containsKey(child.getChild())) {
            return false;
        } else {
            this.m_childMap.put(child.getChild(), child);
            return true;
        }
    }

    /**
     * Adds a child node without any parent relationship to the list of known children. If the child
     * already exists, nothing is done.
     *
     * @param child_id is the new child to put into the bag.
     * @return true if the bag did not already contain the specified element.
     */
    public boolean addChild(String child_id) {
        if (this.m_childMap.containsKey(child_id)) {
            return false;
        } else {
            this.m_childMap.put(child_id, new Child(child_id));
            return true;
        }
    }

    /**
     * Adds a child node with a parent relationship to the list of known children. If the child
     * already exists, but the parent relationship is not known, it will be added to the child's
     * list of parents. If the child already exists and the parent relationship is known, nothing is
     * done.
     *
     * @param child_id is the id of the child for which to modify a parent
     * @param parent_id is the new parent to add to the specified child.
     * @return true if the bag did not already contain the relationship.
     */
    public boolean addChild(String child_id, String parent_id) {
        Child current = (Child) this.m_childMap.get(child_id);
        if (current == null) {
            // unknown child, add to bag
            this.m_childMap.put(child_id, new Child(child_id, parent_id));
            return true;
        } else {
            // child is know, check the parent
            if (!current.getParent(parent_id)) {
                // parent is unknown, add to child
                current.addParent(parent_id);
                return true;
            } else {
                // parent is already known
                return false;
            }
        }
    }

    /** Registers a job node collapsion as a replacement. */
    public String replaceParent(String oldid, String newid) {
        String old = (String) this.m_replace.put(oldid, newid);
        this.m_dirty = true;
        return old;
    }

    /**
     * Accessor: Provides an iterator for the bag of filenames.
     *
     * @return the iterator for <code>Filename</code> elements.
     * @see Filename
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateFilename() {
        return Collections.enumeration(this.m_fileMap.values());
    }

    /**
     * Accessor: Provides an iterator for the bag of jobs.
     *
     * @return the iterator for <code>Job</code> elements.
     * @see Job
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateJob() {
        return Collections.enumeration(this.m_jobMap.values());
    }

    /**
     * Accessor: Provides an iterator for the bag of relationships.
     *
     * @return the iterator for <code>Child</code> elements.
     * @see Child
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateChild() {
        if (this.m_dirty) updateChildren();
        return Collections.enumeration(this.m_childMap.values());
    }

    /**
     * Accessor: Obtains a <code>Filename</code> from its string.
     *
     * @param lfn is the logical filename string to look it up with.
     * @return the filename instance at the specified place.
     * @see #addFilename( Filename )
     * @see #setFilename( Filename )
     */
    public Filename getFilename(String lfn) {
        return (Filename) this.m_fileMap.get(lfn);
    }

    /**
     * Accessor: Obtains the index of filename instances.
     *
     * @return the number of arguments in the filename list.
     * @see Filename
     */
    public int getFilenameCount() {
        return this.m_fileMap.size();
    }

    /**
     * Accessor: Counts the number of jobs in the abstract DAG.
     *
     * @return the number of jobs.
     */
    public int getJobCount() {
        return this.m_jobMap.size();
    }

    /**
     * Access: Counts the number of dependencies in the DAG.
     *
     * @return dependency count, which may be zilch.
     */
    public int getChildCount() {
        if (this.m_dirty) updateChildren();
        return this.m_childMap.size();
    }

    /**
     * Accessor: Obtains the zero-based index.
     *
     * @return a number in the interval [0,size-1].
     * @see #setIndex( int )
     */
    public int getIndex() {
        return this.m_index;
    }

    /**
     * Accessor: Obtains a job by its id from the job list.
     *
     * @return a job or null, if not found.
     * @see #addJob( Job )
     */
    public Job getJob(String jobID) {
        return (Job) this.m_jobMap.get(jobID);
    }

    /**
     * Accessor: Obtains the name of the DAX.
     *
     * @return the name of this DAX, or <code>null</code>, if no name was specified.
     * @see #setName( String )
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Obtains the total number of alternatives. This is the number of DAXes generatable
     * from alternatives.
     *
     * @return a positive natural integer.
     * @see #setSize( int )
     */
    public int getSize() {
        return this.m_size;
    }

    /**
     * Accessor: Obtains the version that will be reported in the DAX.
     *
     * @return the version as a string.
     * @see #setVersion( String )
     * @since 1.7
     */
    public String getVersion() {
        return this.m_version;
    }

    /**
     * Accessor: Provides an iterator for the bag of filenames.
     *
     * @return the iterator for <code>Filename</code> elements.
     * @see Filename
     */
    public Iterator iterateFilename() {
        return this.m_fileMap.values().iterator();
    }

    /**
     * Accessor: Provides an iterator for the bag of jobs.
     *
     * @return the iterator for <code>Job</code> elements.
     * @see Job
     */
    public Iterator iterateJob() {
        return this.m_jobMap.values().iterator();
    }

    /**
     * Accessor: Provides an iterator for the bag of relationships.
     *
     * @return the iterator for <code>Child</code> elements.
     * @see Child
     */
    public Iterator iterateChild() {
        if (this.m_dirty) updateChildren();
        return this.m_childMap.values().iterator();
    }

    /**
     * Accessor: Removes all filename instances.
     *
     * @see Filename
     */
    public void removeAllFilename() {
        this.m_fileMap.clear();
    }

    /**
     * Accessor: Removes a specific logical filename instance from the bag.
     *
     * @param lfn is the logical filename string to refer to a filename.
     * @return the {@link Filename} instance to which this lfn had been mapped in this hashtable, or
     *     <code>null</code> if the lfn did not have a mapping.
     */
    public Filename removeFilename(String lfn) {
        return (Filename) this.m_fileMap.remove(lfn);
    }

    /**
     * Accessor: Overwrites an filename instance with a new one.
     *
     * @param vFilename is the new filename instance, which contains all necessary information.
     */
    public void setFilename(Filename vFilename) {
        this.m_fileMap.put(vFilename.getFilename(), vFilename);
    }

    /**
     * Accessor: Replace this filename instance list with a new list.
     *
     * @param fileArray is the new list of Filename instances
     * @see Filename
     * @deprecated Use the new Collection based interfaces
     */
    public void setFilename(Filename[] fileArray) {
        this.m_fileMap.clear();
        for (int i = 0; i < fileArray.length; i++) {
            this.m_fileMap.put(fileArray[i].getFilename(), fileArray[i]);
        }
    }

    /**
     * Accessor: Replace this filename instance list with a new list.
     *
     * @param files is the new collection of Filename instances
     * @see Filename
     */
    public void setFilename(java.util.Collection files) {
        this.m_fileMap.clear();
        for (Iterator i = files.iterator(); i.hasNext(); ) {
            Filename lfn = (Filename) i.next();
            this.m_fileMap.put(lfn.getFilename(), lfn);
        }
    }

    /**
     * Accessor: Replace this filename instance list with a map.
     *
     * @param files is the new map of Filename instances
     * @see Filename
     */
    public void setFilename(java.util.Map files) {
        this.m_fileMap.clear();
        this.m_fileMap.putAll(files);
    }

    /**
     * Acessor: Sets a new zero-based index for this document. The index is used in conjunction with
     * the total number of documents count.
     *
     * @param index is the new zero-based index of this element.
     * @see #getIndex()
     */
    public void setIndex(int index) {
        this.m_index = index;
    }

    /**
     * Acessor: Sets a new optional name for this document.
     *
     * @param name is the new name.
     * @see #getName()
     */
    public void setName(String name) {
        this.m_name = name;
    }

    /**
     * Acessor: Sets a new total document count in this document. The count is used in conjunction
     * with the zero-based document index.
     *
     * @param size is the new total document count.
     * @see #getSize()
     */
    public void setSize(int size) {
        this.m_size = size;
    }

    /**
     * Acessor: Sets a new version number for this document. The version number is taken by the
     * abstract planner to support a range of valid DAX documents.
     *
     * @param version is the new version number as string composed of two integers separted by a
     *     period.
     * @see #getVersion()
     * @since 1.7
     */
    public void setVersion(String version) {
        this.m_version = version;
    }

    private void updateChildren() {
        // find all child nodes in need of replacement
        TreeMap temp = new TreeMap();
        for (Iterator i = this.m_childMap.values().iterator(); i.hasNext(); ) {
            Child newchild = ((Child) i.next()).updateChild(this.m_replace);
            if (temp.containsKey(newchild.getChild())) {
                // need to merge two definitions
                Child oldchild = (Child) temp.get(newchild.getChild());
                for (Iterator j = oldchild.iterateParent(); j.hasNext(); ) {
                    newchild.addParent((String) j.next());
                }
            }
            // plain insertion
            temp.put(newchild.getChild(), newchild);
        }
        this.m_childMap = temp;
        this.m_dirty = false;
    }

    /**
     * Adjusts all job levels along the search path. Given a starting point, this method will
     * re-iterate the search-tree, and adjust the level of each known job by the specified distance.
     *
     * @param id is the job id to start
     * @param distance is the increment (or decrement for negative).
     * @return number of jobs adjusted?
     */
    public int adjustLevels(String id, int distance) {
        int result = 0;

        if (m_jobMap.containsKey(id)) {
            Job job = (Job) m_jobMap.get(id);
            job.setLevel(job.getLevel() + distance);
            result++;

            // also recursively adjust all known parents of this job
            if (m_childMap.containsKey(id)) {
                Child c = (Child) m_childMap.get(id);
                for (Iterator i = c.iterateParent(); i.hasNext(); ) {
                    result += adjustLevels((String) i.next(), distance);
                }
            }
        }

        // done
        return result;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        // FIXME: default name of a DAX w/o name is "test"
        String daxname = this.m_name != null ? this.m_name : "test";
        stream.write("adag ");
        stream.write(escape(daxname));
        stream.write(" {");
        stream.write(newline);

        stream.write("  count=");
        stream.write((new Integer(this.m_size)).toString());
        stream.write(';');
        stream.write(newline);

        stream.write("  index=");
        stream.write((new Integer(this.m_index)).toString());
        stream.write(';');
        stream.write(newline);

        // part 1: filelist
        stream.write("  files {");
        stream.write(newline);
        for (Iterator i = this.m_fileMap.values().iterator(); i.hasNext(); ) {
            stream.write("    ");
            ((Filename) i.next()).toString(stream);
            stream.write(newline);
        }
        stream.write("  }");
        stream.write(newline);

        // part 2: job list
        stream.write("  jobs {");
        stream.write(newline);
        for (Iterator i = this.m_jobMap.values().iterator(); i.hasNext(); ) {
            ((Job) i.next()).toString(stream);
        }
        stream.write("  }");
        stream.write(newline);

        // part 3: dependencies
        stream.write("  dependencies {");
        stream.write(newline);
        if (this.m_dirty) updateChildren();
        for (Iterator i = this.m_childMap.values().iterator(); i.hasNext(); ) {
            ((Child) i.next()).toString(stream);
        }
        stream.write("  }");
        stream.write(newline);

        stream.write('}');
        stream.write(newline);
        stream.flush();
    }

    /**
     * Writes the header of the XML output. The output contains the special strings to start an XML
     * document, some comments, and the root element. The latter points to the XML schema via XML
     * Instances.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeXMLHeader(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        // FIXME: default name of a DAX w/o name is "test"
        String daxname = this.m_name != null ? this.m_name : "test";

        // intro
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        stream.write(newline);

        // when was this document generated
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated: ");
        stream.write(Currently.iso8601(false));
        stream.write(" -->");
        stream.write(newline);

        // who generated this document
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated by: ");
        stream.write(System.getProperties().getProperty("user.name", "unknown"));
        stream.write(" [");
        stream.write(System.getProperties().getProperty("user.region", "??"));
        stream.write("] -->");
        stream.write(newline);

        // root element with elementary attributes
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("adag xmlns");
        if (namespace != null && namespace.length() > 0) {
            stream.write(':');
            stream.write(namespace);
        }
        stream.write("=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(
                "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(' ');
        stream.write(SCHEMA_LOCATION);
        stream.write('"');
        writeAttribute(stream, " version=\"", SCHEMA_VERSION);

        writeAttribute(stream, " count=\"", Integer.toString(this.m_size));
        writeAttribute(stream, " index=\"", Integer.toString(this.m_index));
        writeAttribute(stream, " name=\"", daxname);

        // added with dax-1.9
        writeAttribute(stream, " jobCount=\"", Integer.toString(this.m_jobMap.size()));
        writeAttribute(stream, " fileCount=\"", Integer.toString(this.m_fileMap.size()));
        writeAttribute(stream, " childCount=\"", Integer.toString(this.m_childMap.size()));

        stream.write('>');
        if (indent != null) stream.write(newline);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String newindent = indent == null ? null : indent + "  ";

        // write prefix
        writeXMLHeader(stream, indent, namespace);

        // part 1: filelist
        stream.write("<!-- part 1: list of all referenced files (may be empty) -->");
        if (indent != null) stream.write(newline);

        for (Iterator i = this.m_fileMap.values().iterator(); i.hasNext(); ) {
            if (indent != null) stream.write(newindent);
            ((Filename) i.next()).shortXML(stream, newindent, namespace, 0x03);
            if (indent != null) stream.write(newline);
        }

        // part 2: job list
        stream.write("<!-- part 2: definition of all jobs (at least one) -->");
        if (indent != null) stream.write(newline);
        for (Iterator i = this.m_jobMap.values().iterator(); i.hasNext(); ) {
            ((Job) i.next()).toXML(stream, newindent, namespace);
        }

        // part 3: dependencies
        if (this.m_dirty) updateChildren();
        stream.write("<!-- part 3: list of control-flow dependencies (may be empty) -->");
        if (indent != null) stream.write(newline);
        for (Iterator i = this.m_childMap.values().iterator(); i.hasNext(); ) {
            ((Child) i.next()).toXML(stream, newindent, namespace);
        }

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("adag>");
        stream.write(newline);
        stream.flush();
    }
}
