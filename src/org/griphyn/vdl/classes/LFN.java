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
package org.griphyn.vdl.classes;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.*;

/**
 * This class captures the logical filename and its linkage. Also, some static methods allow to use
 * the linkage constants outside the class. <code>LFN</code> extends the <code>Leaf</code> class by
 * adding a filename and linkage type.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Leaf
 * @see Text
 * @see Use
 * @see Value
 * @see Scalar
 * @see List
 */
public class LFN extends Leaf implements Cloneable, Serializable {
    /**
     * Linkage type: no linkage, usually used for constants etc. It can also be used to indicate
     * that the linkage is unknown. The NONE linkage does not participate in DAG construction.
     */
    public static final int NONE = 0;

    /** Linkage type: input file. */
    public static final int INPUT = 1;

    /** Linkage type: output file. */
    public static final int OUTPUT = 2;

    /**
     * Linkage type: file is used as input and output. Please note that this linkage does not allow
     * for DAG linking.
     */
    public static final int INOUT = 3;

    /**
     * The filename is the logical name of the file. With the help of the replica location service
     * (RLS), the physical filename is determined by the concrete planner.
     */
    private String m_filename;

    /** The linkage type of the logical file aids the linkage process. */
    private int m_link = LFN.NONE;

    /**
     * Predicate to determine, if an integer is within the valid range for linkage types.
     *
     * @param x is the integer to test for in-intervall.
     * @return true, if the integer satisfies {@link LFN#NONE} &leq; x &leq; {@link LFN#INOUT},
     *     false otherwise.
     */
    public static boolean isInRange(int x) {
        return ((x >= LFN.NONE) && (x <= LFN.INOUT));
    }

    /**
     * Converts an integer into the symbolic linkage type represented by the constant.
     *
     * @param x is the integer with the linkage type to symbolically convert
     * @return a string with the symbolic linkage name, or null, if the constant is out of range.
     */
    public static String toString(int x) {
        switch (x) {
            case LFN.NONE:
                return "none";
            case LFN.INPUT:
                return "input";
            case LFN.OUTPUT:
                return "output";
            case LFN.INOUT:
                return "inout";
            default:
                return null;
        }
    }

    /**
     * Marks a filename for registration in a replica catalog. If marked with true, the replica
     * registration will not take place. This is useful for transient or non-important results.
     * Regular, tracked files are marked with false.
     *
     * @see #m_dontTransfer
     * @see #m_temporary
     */
    private boolean m_dontRegister = false;

    /**
     * Transfer mode: The transfer of the file to the result collector is mandatory. Failure to
     * transfer the file will make the workflow fail.
     */
    public static final int XFER_MANDATORY = 0; // false

    /**
     * Transfer mode: The transfer of the file to the result collector is optional. Failure to
     * transfer the file will <b>not</b> abort the workflow.
     */
    public static final int XFER_OPTIONAL = 1;

    /** Transfer mode: The file will not be transferred to the result collector. */
    public static final int XFER_NOT = 2; // true

    /**
     * Predicate to determine, if an integer is within the valid range for transfer modes.
     *
     * @param x is the integer to test for in-intervall.
     * @return true, if the integer satisfies {@link LFN#XFER_MANDATORY} &leq; x &leq; {@link
     *     LFN#XFER_NOT}, false otherwise.
     */
    public static boolean transferInRange(int x) {
        return ((x >= LFN.XFER_MANDATORY) && (x <= LFN.XFER_NOT));
    }

    /**
     * Converts an integer into the symbolic transfer mode represented by the constant.
     *
     * @param x is the integer with the linkage type to symbolically convert
     * @return a string with the symbolic linkage name, or null, if the constant is out of range.
     */
    public static String transferString(int x) {
        switch (x) {
            case LFN.XFER_MANDATORY:
                return "true";
            case LFN.XFER_OPTIONAL:
                return "optional";
            case LFN.XFER_NOT:
                return "false";
            default:
                return null;
        }
    }

    /** Type of File: Denotes a data file. They are generally looked up in a replica catalog. */
    public static final int TYPE_DATA = 0;

    /**
     * Type of File: Denotes an executable file. They are generally looked up in a transformation
     * catalog.
     */
    public static final int TYPE_EXECUTABLE = 1;

    /** Type of File: Denotes a pattern. They are generally looked up in a pattern catalog. */
    public static final int TYPE_PATTERN = 2;

    /**
     * Predicate to determine, if an integer is within the valid range for type
     *
     * @param x is the integer to test for in-intervall.
     * @return true, if the integer satisfies {@link LFN#TYPE_DATA} &leq; x &leq; {@link
     *     LFN#TYPE_PATTERN}, false otherwise.
     */
    public static boolean typeInRange(int x) {
        return ((x >= LFN.TYPE_DATA) && (x <= LFN.TYPE_PATTERN));
    }

    /**
     * Converts an integer into the symbolic type mode represented by the constant.
     *
     * @param x is the integer with the linkage type to symbolically convert
     * @return a string with the symbolic linkage name, or null, if the constant is out of range.
     */
    public static String typeString(int x) {
        switch (x) {
            case TYPE_DATA:
                return "data";
            case TYPE_EXECUTABLE:
                return "executable";
            case TYPE_PATTERN:
                return "pattern";
            default:
                return null;
        }
    }

    /**
     * Converts a String into the corresponding integer value.
     *
     * @param x is the String to symbolically convert
     * @return an integer with the value or -1 if not valid.
     */
    public static int typeInt(String x) {
        int result = -1;
        if (x == null) {
            return result;
        }

        if (x.equalsIgnoreCase("data")) {
            result = TYPE_DATA;
        } else if (x.equalsIgnoreCase("executable")) {
            result = TYPE_EXECUTABLE;
        } else if (x.equalsIgnoreCase("pattern")) {
            result = TYPE_PATTERN;
        }
        return result;
    }

    /**
     * Marks a filename for transfer to the result collector. If marked with true, the file is
     * usually a temporary file, and will not be transferred to the output collector. Inter-pool
     * transfers may still happen in multi-pool mode. Regular, tracked files are marked with false.
     * Optional transfers have a special mark.
     *
     * @see #m_dontRegister
     * @see #m_temporary
     */
    private int m_dontTransfer = XFER_MANDATORY;

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

    /** The type of the filename, whether it refers to a data, pattern or executable. */
    private int m_type = TYPE_DATA;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public Object clone() {
        return new LFN(
                this.m_filename,
                this.m_link,
                this.m_temporary,
                this.m_dontRegister,
                this.m_dontTransfer,
                this.m_optional);
    }

    /** ctor. */
    public LFN() {
        super();
    }

    /**
     * Default ctor: create an instance with a logical filename. The linkage defaults to {@link
     * LFN#NONE}.
     *
     * @param filename is the logical filename to store.
     */
    public LFN(String filename) {
        super();
        this.m_filename = filename;
        this.m_dontRegister = false;
        this.m_dontTransfer = XFER_MANDATORY;
    }

    /**
     * ctor: create a file with a name and linkage.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @throws IllegalArgumentException if the linkage does not match the legal range.
     */
    public LFN(String filename, int link) throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_dontRegister = false;
        this.m_dontTransfer = XFER_MANDATORY;
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * ctor: create a possibly transient file with a name, linkage and hint.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice. If it is not null, the files
     *     will neither be marked for registration nor for transfer to the output collector.
     * @throws IllegalArgumentException if the linkage does not match the legal range.
     */
    public LFN(String filename, int link, String hint) throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        if ((this.m_temporary = hint) == null) {
            this.m_dontRegister = false;
            this.m_dontTransfer = XFER_MANDATORY;
        } else {
            this.m_dontRegister = true;
            this.m_dontTransfer = XFER_NOT;
        }

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException();
    }

    /**
     * ctor: Creates a filename given almost all specs. This is a backward compatible constructor,
     * as it lacks access to the optional transfer attribute.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param dontRegister whether to to register with a replica catalog.
     * @param dontTransfer whether to transfer the file to the collector.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     * @since 1.21
     * @deprecated
     */
    public LFN(String filename, int link, String hint, boolean dontRegister, int dontTransfer)
            throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_temporary = hint;
        this.m_dontRegister = dontRegister;
        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException("Illegal transfer mode");

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("Illegal linkage type");
    }

    /**
     * ctor: Creates a filename given almost all specs. This is a backward compatible constructor,
     * as it lacks access to the optional transfer attribute.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param dontRegister whether to to register with a replica catalog.
     * @param dontTransfer whether to transfer the file to the collector.
     * @param optional whether the file is optional or required.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     * @since 1.23
     * @deprecated
     */
    public LFN(
            String filename,
            int link,
            String hint,
            boolean dontRegister,
            int dontTransfer,
            boolean optional)
            throws IllegalArgumentException {
        super();
        this.m_filename = filename;
        this.m_temporary = hint;
        this.m_dontRegister = dontRegister;
        this.m_optional = optional;
        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException("Illegal transfer mode");
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("Illegal linkage type");
    }

    // The new constructors that need to be added later, after the deprecation
    // ends for the above constructors. Karan Oct 24, 2007
    /**
     * ctor: Creates a filename given almost all specs. This is a backward compatible constructor,
     * as it lacks access to the optional transfer attribute.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param register whether to to register with a replica catalog.
     * @param transfer whether to transfer the file to the collector.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     */
    //  public LFN( String filename, int link, String hint,
    //	      boolean register, int transfer )
    //  throws IllegalArgumentException
    //  {
    //    super();
    //    this.m_filename = filename;
    //    this.m_temporary = hint;
    //    this.m_dontRegister = !register;
    //    if ( LFN.transferInRange( transfer ) )
    //      this.m_dontTransfer = transfer;
    //    else
    //      throw new IllegalArgumentException("Illegal transfer mode");
    //
    //    if ( LFN.isInRange(link) )
    //      this.m_link = link;
    //    else
    //      throw new IllegalArgumentException("Illegal linkage type");
    //  }

    /**
     * ctor: Creates a filename given almost all specs. This is a backward compatible constructor,
     * as it lacks access to the optional transfer attribute.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param register whether to to register with a replica catalog.
     * @param transfer whether to transfer the file to the collector.
     * @param optional whether the file is optional or required.
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     */
    //  public LFN( String filename, int link, String hint,
    //	      boolean register, int transfer, boolean optional )
    //    throws IllegalArgumentException
    //  {
    //    super();
    //    this.m_filename = filename;
    //    this.m_temporary = hint;
    //    this.m_dontRegister = !register;
    //    this.m_optional = optional;
    //    if ( LFN.transferInRange( transfer ) )
    //      this.m_dontTransfer = transfer;
    //    else
    //      throw new IllegalArgumentException("Illegal transfer mode");
    //    if ( LFN.isInRange(link) )
    //      this.m_link = link;
    //    else
    //      throw new IllegalArgumentException("Illegal linkage type");
    //  }

    /**
     * ctor: Creates a filename given almost all specs. This is a backward compatible constructor,
     * as it lacks access to the optional transfer attribute.
     *
     * @param filename is the logical filename to store.
     * @param link is the linkage of the file to remember.
     * @param hint is an expression for a temporary filename choice.
     * @param dontRegister whether to to register with a replica catalog.
     * @param dontTransfer whether to transfer the file to the collector.
     * @param optional whether the file is optional or required.
     * @param type whether the file is data|executable|pattern
     * @throws IllegalArgumentException if the linkage does not match the legal range, or the
     *     transfer mode does not match its legal range.
     * @since 1.23
     */
    public LFN(
            String filename,
            int link,
            String hint,
            boolean dontRegister,
            int dontTransfer,
            boolean optional,
            int type)
            throws IllegalArgumentException {
        this(filename, link, hint, dontRegister, dontTransfer, optional);

        if (LFN.typeInRange(type)) this.m_type = type;
        else throw new IllegalArgumentException("Illegal File type");
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
     * @return the linkage type of the current object. Note that <code>LFN</code> objects
     *     <i>default</i> to no linkage.
     * @see #setLink(int)
     */
    public int getLink() {
        return this.m_link;
    }

    /**
     * Accessor: Obtains the logical filename of the object.
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
     * Accessor: Obtains the predicate on registring with a replica catalog.
     *
     * @return false if the file will be registered with a replica catalog.
     * @see #setRegister( boolean )
     * @see #getRegister()
     * @deprecated
     * @since 1.21
     */
    public boolean getDontRegister() {
        return this.m_dontRegister;
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
     * Accessor: Obtains the transfering mode.
     *
     * @return false if the file will be tranferred to an output collector.
     * @deprecated
     * @see #getTransfer()
     * @see #setTransfer( int )
     * @since 1.21
     */
    public int getDontTransfer() {
        return this.m_dontTransfer;
    }

    /**
     * Acessor: Obtains the optionality of the file.
     *
     * @return false, if the file is required, or true, if it is optional.
     * @see #setOptional( boolean )
     * @since 1.23
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

    //  /**
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
     * @param fn is the new logical filename.
     * @see #getFilename()
     */
    public void setFilename(String fn) {
        this.m_filename = fn;
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
     * Accessor: Sets the predicate on the type of the LFN
     *
     * @param type the type of LFN
     * @see #getType( )
     * @since 2.1
     */
    public void setType(int type) {
        if (typeInRange(type)) {
            this.m_type = type;
        } else {
            throw new IllegalArgumentException("Invalid LFN type " + type);
        }
    }

    /**
     * Accessor: Sets the predicate on registring with a replica catalog.
     *
     * @param dontRegister is false, if the file should be registered with a replica catalog.
     * @see #getDontRegister()
     * @since 1.21
     * @deprecated
     * @see #setRegister( boolean )
     */
    public void setDontRegister(boolean dontRegister) {
        this.m_dontRegister = dontRegister;
    }

    /**
     * Accessor: Sets the transfer mode.
     *
     * @param transfer the transfer flag
     * @exception IllegalArgumentException if the transfer mode is outside its legal range.
     * @see #getTransfer( )
     * @see LFN#XFER_MANDATORY
     * @see LFN#XFER_OPTIONAL
     * @see LFN#XFER_NOT
     * @since 2.1
     */
    public void setTransfer(int transfer) throws IllegalArgumentException {
        if (LFN.transferInRange(transfer)) this.m_dontTransfer = transfer;
        else throw new IllegalArgumentException();
    }

    /**
     * Accessor: Sets the transfer mode.
     *
     * @param dontTransfer is false, if the file should be transferred to the output collector.
     * @exception IllegalArgumentException if the transfer mode is outside its legal range.
     * @deprecated
     * @see #getDontTransfer( )
     * @see LFN#XFER_MANDATORY
     * @see LFN#XFER_OPTIONAL
     * @see LFN#XFER_NOT
     * @since 1.21
     */
    public void setDontTransfer(int dontTransfer) throws IllegalArgumentException {
        if (LFN.transferInRange(dontTransfer)) this.m_dontTransfer = dontTransfer;
        else throw new IllegalArgumentException();
    }

    /**
     * Acessor: Sets the optionality of the file.
     *
     * @param optional false, if the file is required, or true, if it is optional.
     * @see #getOptional()
     * @since 1.23
     */
    public void setOptional(boolean optional) {
        this.m_optional = optional;
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
     * Predicate to determine, if the output can be abbreviated. Filenames can be abbreviated, if
     * one of these two conditions are met: The hint is <code>null</code> and dontRegister is false
     * and dontTransfer is mandatory, or the hint exists, and dontRegister is true and dontTransfer
     * is no transfer.
     *
     * @param temp is the temporary hint
     * @param dr is the value of dontRegister
     * @param dt is the value of dontTransfer
     * @param opt is whether a given file is optional or not
     * @return true, if the filename can use abbreviated mode
     */
    public static boolean abbreviatable(String temp, boolean dr, int dt, boolean opt) {
        if (opt) return false;
        else
            return ((temp == null && !dr && dt == LFN.XFER_MANDATORY)
                    || (temp != null && dr && dt == LFN.XFER_NOT));
    }

    /**
     * Convenience function to call the static test, if a filename can use the abbreviated notation.
     *
     * @return true, if abbreviatable notation is possible.
     * @see #abbreviatable( String, boolean, int, boolean )
     */
    private boolean abbreviatable() {
        return LFN.abbreviatable(
                this.m_temporary, this.m_dontRegister, this.m_dontTransfer, this.m_optional);
    }

    /**
     * Convert the logical filename and linkage into something human readable. The output is also
     * slightly nudged towards machine parsability.
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
            // new mode, generate appendices
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
     * Prints the current content onto the stream.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @throws IOException if something happens to the stream.
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
            // new mode, generate appendices
            stream.write('|');
            if (this.m_optional) stream.write('o');
            if (!this.m_dontRegister) stream.write('r');
            if (this.m_dontTransfer != LFN.XFER_NOT)
                stream.write(this.m_dontTransfer == LFN.XFER_OPTIONAL ? 'T' : 't');
        }

        stream.write("}");
    }

    /**
     * Dumps the state of the current element as XML output. This method converts the data into
     * pretty-printed XML output meant for machine consumption.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     */
    public String toXML(String indent) {
        // slight over-allocation is without harm
        StringBuffer result = new StringBuffer(128);

        if (indent != null) result.append(indent);
        result.append("<lfn file=\"").append(quote(this.m_filename, true));
        result.append("\" link=\"").append(LFN.toString(this.m_link));
        result.append("\" register=\"").append(Boolean.toString(!this.m_dontRegister));
        result.append("\" transfer=\"").append(LFN.transferString(this.m_dontTransfer));
        result.append("\" optional=\"").append(Boolean.toString(this.m_optional));

        result.append("\" type=\"").append(LFN.typeString(this.m_type));
        if (this.m_temporary != null) {
            result.append("\" temporaryHint=\"");
            result.append(quote(this.m_temporary, true));
        }
        result.append("\"/>");
        if (indent != null) result.append(System.getProperty("line.separator", "\r\n"));

        return result.toString();
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you use a buffered writer.
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
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("lfn");
        writeAttribute(stream, " file=\"", this.m_filename);
        writeAttribute(stream, " link=\"", LFN.toString(this.m_link));
        writeAttribute(stream, " register=\"", Boolean.toString(!this.m_dontRegister));
        writeAttribute(stream, " transfer=\"", LFN.transferString(this.m_dontTransfer));
        writeAttribute(stream, " optional=\"", Boolean.toString(this.m_optional));
        writeAttribute(stream, " type=\"", LFN.typeString(this.m_type));

        // null-safe
        writeAttribute(stream, " temporaryHint=\"", this.m_temporary);
        stream.write("/>");
        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
