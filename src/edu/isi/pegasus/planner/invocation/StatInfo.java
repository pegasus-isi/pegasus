/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
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
package edu.isi.pegasus.planner.invocation;

import edu.isi.pegasus.common.util.Currently;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class is the container for the results of a call to either stat() or fstat(). Not all stat
 * information is kept.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class StatInfo extends Invocation // implements Cloneable
{
    /** Is the number for the file mode. This is originally an octal string. */
    private int m_mode;

    /** Denotes the size of the file. Files can grow rather large. */
    private long m_size;

    /** We store the inode number, which let's us reference a file uniquely per filesystem. */
    private long m_inode;

    /** Stores the number of hard links to the file. */
    private long m_nlink;

    /** Stores the blocksize of the file. */
    private long m_blksize;

    /** Stores the number of blocks of the file. */
    private long m_blocks;

    /** Contains the last access time timestamp. */
    private Date m_atime;

    /** Contains the creation time timestamp. */
    private Date m_ctime;

    /** Contains the last modification time timestamp; */
    private Date m_mtime;

    /** user id of the owner of the file. */
    private int m_uid;

    /** symbolical user name of the effective user. */
    private String m_user;

    /** group id of the owner of the file. */
    private int m_gid;

    /** symbolical group name of the effective user. */
    private String m_group;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public StatInfo() {
        m_uid = m_gid = -1;
        m_atime = m_ctime = m_mtime = new Date();
    }

    /**
     * Accessor
     *
     * @see #setMode(int)
     */
    public int getMode() {
        return this.m_mode;
    }

    /**
     * Accessor.
     *
     * @param mode
     * @see #getMode()
     */
    public void setMode(int mode) {
        this.m_mode = mode;
    }

    /**
     * Accessor
     *
     * @see #setSize(long)
     */
    public long getSize() {
        return this.m_size;
    }

    /**
     * Accessor.
     *
     * @param size
     * @see #getSize()
     */
    public void setSize(long size) {
        this.m_size = size;
    }

    /**
     * Accessor
     *
     * @see #setINode(long)
     */
    public long getINode() {
        return this.m_inode;
    }

    /**
     * Accessor.
     *
     * @param inode
     * @see #getINode()
     */
    public void setINode(long inode) {
        this.m_inode = inode;
    }

    /**
     * Accessor
     *
     * @see #setLinkCount(long)
     */
    public long getLinkCount() {
        return this.m_nlink;
    }

    /**
     * Accessor.
     *
     * @param nlink
     * @see #getLinkCount()
     */
    public void setLinkCount(long nlink) {
        this.m_nlink = nlink;
    }

    /**
     * Accessor
     *
     * @see #setBlockSize(long)
     */
    public long getBlockSize() {
        return this.m_blksize;
    }

    /**
     * Accessor.
     *
     * @param blksize
     * @see #getBlockSize()
     */
    public void setBlockSize(long blksize) {
        this.m_blksize = blksize;
    }

    /**
     * Accessor
     *
     * @see #setBlocks(long)
     */
    public long getBlocks() {
        return this.m_blocks;
    }

    /**
     * Accessor.
     *
     * @param blocks
     * @see #getBlocks()
     */
    public void setBlocks(long blocks) {
        this.m_blocks = blocks;
    }

    /**
     * Accessor
     *
     * @see #setAccessTime(Date)
     */
    public Date getAccessTime() {
        return this.m_atime;
    }

    /**
     * Accessor.
     *
     * @param atime
     * @see #getAccessTime()
     */
    public void setAccessTime(Date atime) {
        this.m_atime = atime;
    }

    /**
     * Accessor
     *
     * @see #setCreationTime(Date)
     */
    public Date getCreationTime() {
        return this.m_ctime;
    }

    /**
     * Accessor.
     *
     * @param ctime
     * @see #getCreationTime()
     */
    public void setCreationTime(Date ctime) {
        this.m_ctime = ctime;
    }

    /**
     * Accessor
     *
     * @see #setModificationTime(Date)
     */
    public Date getModificationTime() {
        return this.m_mtime;
    }

    /**
     * Accessor.
     *
     * @param mtime
     * @see #getModificationTime()
     */
    public void setModificationTime(Date mtime) {
        this.m_mtime = mtime;
    }

    /**
     * Accessor
     *
     * @see #setUID(int)
     */
    public int getUID() {
        return this.m_uid;
    }

    /**
     * Accessor.
     *
     * @param uid
     * @see #getUID()
     */
    public void setUID(int uid) {
        this.m_uid = uid;
    }

    /**
     * Accessor
     *
     * @see #setUser(String)
     */
    public String getUser() {
        return this.m_user;
    }

    /**
     * Accessor.
     *
     * @param user
     * @see #getUser()
     */
    public void setUser(String user) {
        this.m_user = user;
    }

    /**
     * Accessor
     *
     * @see #setGID(int)
     */
    public int getGID() {
        return this.m_gid;
    }

    /**
     * Accessor.
     *
     * @param gid
     * @see #getGID()
     */
    public void setGID(int gid) {
        this.m_gid = gid;
    }

    /**
     * Accessor
     *
     * @see #setGroup(String)
     */
    public String getGroup() {
        return this.m_group;
    }

    /**
     * Accessor.
     *
     * @param group
     * @see #getGroup()
     */
    public void setGroup(String group) {
        this.m_group = group;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vds-support@griphyn.org");
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
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("statinfo");
        writeAttribute(stream, " mode=\"0", Integer.toOctalString(m_mode));
        writeAttribute(stream, " size=\"", Long.toString(m_size));
        writeAttribute(stream, " inode=\"", Long.toString(m_inode));
        writeAttribute(stream, " nlink=\"", Long.toString(m_nlink));
        writeAttribute(stream, " blksize=\"", Long.toString(m_blksize));
        writeAttribute(stream, " blocks=\"", Long.toString(m_blocks));
        writeAttribute(stream, " mtime=\"", Currently.iso8601(false, true, false, m_mtime));
        writeAttribute(stream, " atime=\"", Currently.iso8601(false, true, false, m_atime));
        writeAttribute(stream, " ctime=\"", Currently.iso8601(false, true, false, m_ctime));

        writeAttribute(stream, " uid=\"", Integer.toString(m_uid));
        if (this.m_user != null && this.m_user.length() > 0)
            writeAttribute(stream, " user=\"", this.m_user);
        writeAttribute(stream, " gid=\"", Integer.toString(m_gid));
        if (this.m_group != null && this.m_group.length() > 0)
            writeAttribute(stream, " group=\"", this.m_group);

        // done
        stream.write("/>");
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
