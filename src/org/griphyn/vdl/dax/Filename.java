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

import java.io.IOException;
import java.io.Writer;
import org.griphyn.vdl.classes.LFN;

/**
 * This class captures the logical filename and its linkage. Also, some static methods allow to use
 * the linkage constants outside the class.
 *
 * <p><code>Filename</code> extends the <code>Leaf</code> class by adding a filename, linkage type,
 * temporary pattern, and management attributes.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Leaf
 * @see PseudoText
 */
public class Filename extends Leaf implements Cloneable {
    /**
     * The filename is the logical name of the file. With the help of the replica location service
     * (RLS), the physical filename is determined by the concrete planner.
     */
    private String m_filename = null;

    /** The linkage type of the logical file aids the linkage process. */
    private int m_link = LFN.NONE;

    /**
     * Marks a filename for registration in a replica catalog. If marked with false, the replica
     * registration will not take place. This is useful for transient or non-important results.
     *
     * @see #m_dontTransfer
     * @see #m_temporary
     */
    private boolean m_dontRegister = false;

    /**
     * Marks a filename for transfer to the result collector. If marked with false, the file is
     * usually a temporary file, and will not be transferred to the output collector. Inter-pool
     * transfers may still happen in multi-pool mode. In optional mode, failure to transfer due to
     * missing source file will not be fatal.
     *
     * @see #m_dontRegister
     * @see #m_temporary
     */
    private int m_dontTransfer = LFN.XFER_MANDATORY;

    /**
     * If a filename is marked transient, the higher level planners might have some notion where to
     * place it, or how to name it. Lower level planners are not necessarily required to follow this
     * hint.
     *
     * @see #m_dontRegister
     * @see #m_dontTransfer
     */
    private String m_temporary = null;

    /**
     * If a filename is marked as optional, it's non-existence must not stop a workflow. Regular
     * files, however, are not optional.
     */
    private boolean m_optional = false;

    /*
     * Records the VDL variable that was responsible for generating this
     * logical filename.
     */
    private String m_variable = null;

    /** The type of the filename, whether it refers to a data, pattern or executable. */
    private int m_type = LFN.TYPE_DATA;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        return new Filename(
                this.m_filename,
                this.m_link,
                this.m_temporary,
                this.m_dontRegister,
                this.m_dontTransfer,
                this.m_variable,
                this.m_optional);
    }

    /** Default ctor: create a hollow instance which needs to be filled with content. */
    public Filename() {
        super();
    }

    /**
     * Default ctor: create an instance with a logical filename. The linkage defaults to {@link
     * org.griphyn.vdl.classes.LFN#NONE}.
     *
     * @param filename is the logical filename to store.
     */
    public Filename(String filename) {
        super();
        this.m_filename = filename;
        this.m_dontRegister = false;
        this.m_dontTransfer = LFN.XFER_MANDATORY;
    }

    /**
     * ctor: create a file with a name and linkage.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @throws IllegalArgumentException if the linkage does not match the legal range.
     */
    public Filename(String filename, int link) throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_dontRegister = false;
        this.m_dontTransfer = LFN.XFER_MANDATORY;
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * ctor: create a transient file with a name, linkage and hint.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is the transient filename. If null, the file is regular, if set, the file is
     *     assumed to be neither registered not transferred.
     * @exception IllegalArgumentException if the linkage does not match the legal range.
     */
    public Filename(String filename, int link, String hint) throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_temporary = hint;
        if ((this.m_temporary = hint) == null) {
            this.m_dontRegister = false;
            this.m_dontTransfer = LFN.XFER_MANDATORY;
        } else {
            this.m_dontRegister = true;
            this.m_dontTransfer = LFN.XFER_NOT;
        }

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * ctor: Creates a filename given all specs.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param dontRegister whether to to register with a replica catalog.
     * @param dontTransfer whether to transfer the file to the collector.
     * @param variable is the variable that is responsible for this LFN.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     * @since 1.6
     */
    public Filename(
            String filename,
            int link,
            String hint,
            boolean dontRegister,
            int dontTransfer,
            String variable)
            throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_temporary = hint;
        this.m_dontRegister = dontRegister;
        this.m_variable = variable;

        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException("Illegal transfer mode");

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("Illegal linkage type");
    }

    /**
     * ctor: Creates a filename given all specs.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param dontRegister whether to to register with a replica catalog.
     * @param dontTransfer whether to transfer the file to the collector.
     * @param variable is the variable that is responsible for this LFN.
     * @param optional records the optionality of a given file.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     * @since 1.8
     */
    public Filename(
            String filename,
            int link,
            String hint,
            boolean dontRegister,
            int dontTransfer,
            String variable,
            boolean optional)
            throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_temporary = hint;
        this.m_dontRegister = dontRegister;
        this.m_variable = variable;
        this.m_optional = optional;

        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException("Illegal transfer mode");

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("Illegal linkage type");
    }

    /**
     * convenience ctor: create a DAX filename from a VDLx filename.
     *
     * @param lfn is a VDLx logical filename.
     */
    public Filename(LFN lfn) throws IllegalArgumentException {
        super();
        this.m_filename = lfn.getFilename();
        this.m_link = lfn.getLink();
        this.m_temporary = lfn.getTemporary();
        this.m_dontRegister = !lfn.getRegister();
        this.m_dontTransfer = lfn.getTransfer();
        this.m_optional = lfn.getOptional();
        this.m_type = lfn.getType();
    }

    //   /**
    //    * @deprecated Use the finer control of {@link #getDontRegister}
    //    * and {@link #getDontTransfer}.
    //    *
    //    * @return true, if the current filename instance points to
    //    * a transient (dontRegister, dontTransfer) file. False for all other
    //    * cases.
    //    */
    //   public boolean getIsTransient()
    //   {
    //     return ( this.m_dontRegister && this.m_dontTransfer );
    //   }

    /**
     * Accessor: Obtains the linkage type from the object.
     *
     * @return the linkage type of the current object. Note that <code>Filename</code> constructor
     *     defaults to no linkage.
     * @see #setLink(int)
     */
    public int getLink() {
        return this.m_link;
    }

    /**
     * Accessor: Obtains the logical filename for this instance.
     *
     * @return the logical filename.
     * @see #setFilename( java.lang.String )
     */
    public String getFilename() {
        return this.m_filename;
    }

    /**
     * Accessor: Obtains the predicate on registring with a replica catalog.
     *
     * @return false if the file will be registered with a replica catalog.
     * @deprecated
     * @see #getRegister( )
     * @since 1.6
     */
    public boolean getDontRegister() {
        return this.m_dontRegister;
    }

    /**
     * Accessor: Obtains the predicate on registring with a replica catalog.
     *
     * @return true if the file will be registered with a replica catalog.
     * @see #setRegister( boolean )
     * @since 2.1
     */
    public boolean getRegister() {
        return !this.m_dontRegister;
    }

    /**
     * Accessor: Returns the predicate on the type of the LFN
     *
     * @return the type of LFN
     * @see #setType( int )
     * @since 2.1
     */
    public int getType() {
        return this.m_type;
    }

    /**
     * Accessor: Obtains the transfer mode.
     *
     * @return false if the file will be tranferred to an output collector.
     * @deprecated
     * @see #getTransfer()
     * @since 1.6
     */
    public int getDontTransfer() {
        return this.m_dontTransfer;
    }

    /**
     * Accessor: Obtains the transfering mode.
     *
     * @return true if the file will be tranferred to an output collector.
     * @see #setTransfer( int )
     * @since 2.1
     */
    public int getTransfer() {
        return this.m_dontTransfer;
    }

    /**
     * Acessor: Obtains the optionality of the file.
     *
     * @return false, if the file is required, or true, if it is optional.
     * @see #setOptional( boolean )
     * @since 1.8
     */
    public boolean getOptional() {
        return this.m_optional;
    }

    /**
     * Accessor: Obtains the file name suggestion for a transient file. If a filename is marked
     * transient, the higher level planners might have some notion where to place it, or how to name
     * it. Lower level planners are not necessarily required to follow this hint.
     *
     * @return the transient name suggestion of the file. The current settings will always be
     *     returned, regardless of the transiency state of the file.
     * @see #setTemporary(String)
     */
    public String getTemporary() {
        return this.m_temporary;
    }

    /**
     * Accessor: Obtains the responsible variable.
     *
     * @return the variable responsible for setting this LFN.
     * @see #setVariable( String )
     * @since 1.7
     */
    public String getVariable() {
        return this.m_variable;
    }

    //   /**
    //    * @deprecated Use the finer control of {@link #setDontRegister} and
    //    * {@link #setDontTranfer} for transiency control.
    //    *
    //    * @param transient is the transience state of this filename instance.
    //    * dontRegister and dontTransfer will both be set to the value of
    //    * transient.
    //    *
    //    * @see #getIsTransient()
    //    */
    //   public void setIsTransient( boolean isTransient )
    //   { this.m_dontRegister = this.m_dontTransfer = isTransient; }

    /**
     * Accessor: Sets the linkage type.
     *
     * @param link is the new linkage type to use. Please note that it must match the range of legal
     *     values.
     * @throws IllegalArgumentException if the range is beyong legal values.
     * @see #getLink()
     */
    public void setLink(int link) throws IllegalArgumentException {
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * Accessor: Sets the filename
     *
     * @param name is the new logical filename.
     * @see #getFilename()
     */
    public void setFilename(String name) {
        this.m_filename = name;
    }

    /**
     * Accessor: Sets the predicate on registring with a replica catalog.
     *
     * @param dontRegister is false, if the file should be registered with a replica catalog.
     * @deprecated
     * @see #setRegister(boolean)
     * @since 1.6
     */
    public void setDontRegister(boolean dontRegister) {
        this.m_dontRegister = dontRegister;
    }

    /**
     * Accessor: Sets the predicate on registring with a replica catalog.
     *
     * @param register is true, if the file should be registered with a replica catalog.
     * @see #getRegister( )
     * @since 2.1
     */
    public void setRegister(boolean register) {
        this.m_dontRegister = !register;
    }

    /**
     * Accessor: Sets the transfer mode.
     *
     * @param dontTransfer is false, if the file should be transferred to the output collector.
     * @exception IllegalArgumentException if the transfer mode is outside its legal range.
     * @deprecated
     * @see #setTransfer(int)
     * @see org.griphyn.vdl.classes.LFN#XFER_MANDATORY
     * @see org.griphyn.vdl.classes.LFN#XFER_OPTIONAL
     * @see org.griphyn.vdl.classes.LFN#XFER_NOT
     * @since 1.6
     */
    public void setDontTransfer(int dontTransfer) throws IllegalArgumentException {
        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException();
    }

    /**
     * Accessor: Sets the transfer mode.
     *
     * @param transfer the transfer flag
     * @exception IllegalArgumentException if the transfer mode is outside its legal range.
     * @see #getTransfer( )
     * @see org.griphyn.vdl.classes.LFN#XFER_MANDATORY
     * @see org.griphyn.vdl.classes.LFN#XFER_OPTIONAL
     * @see org.griphyn.vdl.classes.LFN#XFER_NOT
     * @since 2.1
     */
    public void setTransfer(int transfer) throws IllegalArgumentException {
        if (LFN.transferInRange(transfer)) this.m_dontTransfer = transfer;
        else throw new IllegalArgumentException();
    }

    /**
     * Acessor: Sets the optionality of the file.
     *
     * @param optional false, if the file is required, or true, if it is optional.
     * @see #getOptional()
     * @since 1.8
     */
    public void setOptional(boolean optional) {
        this.m_optional = optional;
    }

    /**
     * Accessor: Sets the predicate on the type of the LFN
     *
     * @param type the type of LFN
     * @see #getType( )
     * @since 2.1
     */
    public void setType(int type) {
        if (LFN.typeInRange(type)) {
            this.m_type = type;
        } else {
            throw new IllegalArgumentException("Invalid LFN type " + type);
        }
    }

    /**
     * Accessor: Sets a file name suggestion for a transient file. If a filename is marked
     * transient, the higher level planners might have some notion where to place it, or how to name
     * it. Lower level planners are not necessarily required to follow this hint.
     *
     * @param name is a transient name suggestion for this filename instance. No automatic marking
     *     of transiency will be done!
     * @see #getTemporary()
     */
    public void setTemporary(String name) {
        this.m_temporary = name;
    }

    /**
     * Accessor: Sets the responsible variable.
     *
     * @param variable the variable responsible for setting this LFN.
     * @see #getVariable()
     * @since 1.7
     */
    public void setVariable(String variable) {
        this.m_variable = variable;
    }

    /**
     * Convenience function to call the static test, if a filename can use the abbreviated notation.
     *
     * @return true, if abbreviatable notation is possible.
     * @see org.griphyn.vdl.classes.LFN#abbreviatable( String, boolean, int, boolean )
     */
    private boolean abbreviatable() {
        return LFN.abbreviatable(
                this.m_temporary, this.m_dontRegister, this.m_dontTransfer, this.m_optional);
    }

    /**
     * Convert the logical filename and linkage into something human readable. The output is also
     * slightly nudged towards machine parsability. This method overwrites the inherited methods
     * since it appears to be faster to do it this way.
     *
     * <p>
     *
     * @return a textual description of the element and its attributes.
     */
    public String toString() {
        // slight over-allocation is without harm
        StringBuffer result = new StringBuffer(this.m_filename.length() + 32);

        result.append("@{");
        result.append(LFN.toString(this.m_link));
        result.append(":\"");
        result.append(escape(this.m_filename));
        if (this.m_temporary != null) {
            result.append("\":\"");
            result.append(escape(this.m_temporary));
        }
        result.append('"');

        if (!abbreviatable()) {
            result.append('|');
            if (this.m_optional) result.append('o');
            if (!this.m_dontRegister) result.append('r');
            if (this.m_dontTransfer != LFN.XFER_NOT)
                result.append(this.m_dontTransfer == LFN.XFER_OPTIONAL ? 'T' : 't');
        }
        result.append('}');
        return result.toString();
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        stream.write("@{");
        stream.write(LFN.toString(this.m_link));
        stream.write(":\"");
        stream.write(escape(this.m_filename)); // risk NullPointerException
        if (this.m_temporary != null) {
            stream.write("\":\"");
            stream.write(escape(this.m_temporary));
        }
        stream.write('"');

        if (!abbreviatable()) {
            stream.write('|');
            if (this.m_optional) stream.write('o');
            if (!this.m_dontRegister) stream.write('r');
            if (this.m_dontTransfer != LFN.XFER_NOT)
                stream.write(this.m_dontTransfer == LFN.XFER_OPTIONAL ? 'T' : 't');
        }

        stream.write("}");
    }

    /**
     * Dumps the state of the filename as PlainFilenameType or StdioFilenameType without the
     * transiency information.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @param flag 0x01: also dump the linkage information, 0x02: also dump optionality
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     */
    public String shortXML(String indent, String namespace, int flag) {
        // slight over-allocation is without harm
        StringBuffer result = new StringBuffer(this.m_filename.length() + 32);

        if (indent != null) result.append(indent);
        result.append('<');
        if (namespace != null && namespace.length() > 0) result.append(namespace).append(':');
        result.append("filename file=\"").append(quote(this.m_filename, true));
        if (this.m_variable != null && this.m_variable.length() > 0)
            result.append("\" varname=\"").append(quote(this.m_variable, true));
        if ((flag & 0x01) == 0x01) result.append("\" link=\"").append(LFN.toString(this.m_link));
        if ((flag & 0x02) == 0x02 && this.m_optional) result.append("\" optional=\"true\"");
        result.append("\"/>");

        return result.toString();
    }

    /**
     * Dumps the state of the current element as XML output. This method converts the data into
     * pretty-printed XML output meant for machine consumption. This method overwrites the inherited
     * methods since it appears to be faster to do it this way.
     *
     * <p>
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     */
    public String toXML(String indent, String namespace) {
        // slight over-allocation is without harm
        StringBuffer result = new StringBuffer(this.m_filename.length() + 128);

        if (indent != null) result.append(indent);
        result.append('<');
        if (namespace != null && namespace.length() > 0) result.append(namespace).append(':');
        result.append("filename file=\"").append(quote(this.m_filename, true));
        result.append("\" link=\"").append(LFN.toString(this.m_link));
        result.append("\" register=\"").append(Boolean.toString(this.getRegister()));
        result.append("\" transfer=\"").append(LFN.transferString(this.getTransfer()));
        result.append("\" optional=\"").append(Boolean.toString(this.m_optional));
        result.append("\" type=\"").append(LFN.typeString(this.getType()));

        if (this.m_temporary != null) {
            result.append("\" temporaryHint=\"");
            result.append(quote(this.m_temporary, true));
        }
        result.append("\"/>");

        return result.toString();
    }

    /**
     * Dumps the state of the filename as PlainFilenameType or StdioFilenameType without the
     * transiency information.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @param flag if 0x01, dump linkage, if 0x02 is set, dump optionality.
     * @exception IOException if something fishy happens to the stream.
     */
    public void shortXML(Writer stream, String indent, String namespace, int flag)
            throws IOException {
        //    if ( indent != null && indent.length() > 0 ) stream.write( indent );
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("filename");
        writeAttribute(stream, " file=\"", this.m_filename);
        if (this.m_variable != null && this.m_variable.length() > 0)
            writeAttribute(stream, " varname=\"", this.m_variable);
        if ((flag & 0x01) == 0x01) writeAttribute(stream, " link=\"", LFN.toString(this.m_link));
        if ((flag & 0x02) == 0x02 && this.m_optional) stream.write(" optional=\"true\"");
        stream.write("/>");
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
        //    if ( indent != null && indent.length() > 0 ) stream.write( indent );
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("filename");
        writeAttribute(stream, " varname=\"", this.m_variable);
        writeAttribute(stream, " file=\"", this.m_filename);
        writeAttribute(stream, " link=\"", LFN.toString(this.m_link));
        writeAttribute(stream, " register=\"", Boolean.toString(this.getRegister()));
        writeAttribute(stream, " dontTransfer=\"", LFN.transferString(this.getTransfer()));
        writeAttribute(stream, " optional=\"", Boolean.toString(this.m_optional));
        writeAttribute(stream, " type=\"", LFN.typeString(this.getType()));

        if (this.m_temporary != null) writeAttribute(stream, " temporaryHint=\"", this.m_temporary);

        stream.write("/>");
    }
}
