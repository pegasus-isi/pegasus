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
package org.griphyn.vdl.dbschema;

import java.sql.*;
import org.griphyn.vdl.annotation.QueryTree;
import org.griphyn.vdl.annotation.Tuple;

/**
 * This common schema interface defines the schemas in which the abstraction layers access the
 * metadata associated with VDC elements. This layer is independent of the implementing database,
 * and does so by going via the database driver class API.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 * @see org.griphyn.vdl.dbdriver
 */
public interface Annotation extends Catalog {
    /** Annotations for transformations. */
    public static final int CLASS_TRANSFORMATION = 0;

    /** Annotations for derivations. */
    public static final int CLASS_DERIVATION = 1;

    /** Annotations for call statements. */
    public static final int CLASS_CALL = 2;

    /** Annotations for formal arguments. */
    public static final int CLASS_DECLARE = 3;

    /** Annotations for logical filenames. */
    public static final int CLASS_FILENAME = 4;

    /**
     * Annotates any of the annotatable classes with the specified tuple. This is an interface
     * method to the various class-specific methods.
     *
     * @param primary is the primary object specifier for the class. According to the type, this is
     *     either the FQDI, or the filename.
     * @param secondary is a helper argument for annotations to calls and formal arguments, and
     *     should be null for all other classes. For calls, the argument must be packed into {@link
     *     java.lang.Integer}.
     * @param kind defines the kind/class of object to annotate.
     * @param annotation is the value to place into the class.
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see #saveAnnotationTransformation( String, Tuple, boolean )
     * @see #saveAnnotationDerivation( String, Tuple, boolean )
     * @see #saveAnnotationCall( String, int, Tuple, boolean )
     * @see #saveAnnotationDeclare( String, String, Tuple, boolean )
     * @see #saveAnnotationFilename( String, Tuple, boolean )
     */
    public long saveAnnotation(
            String primary, Object secondary, int kind, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Annotates a transformation with a tuple.
     *
     * @param fqdi is the FQDI to annotate
     * @param annotation is the value to place
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see org.griphyn.vdl.classes.Transformation
     */
    public long saveAnnotationTransformation(String fqdi, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Annotates a derivation with a tuple.
     *
     * @param fqdi is the FQDI to annotate
     * @param annotation is the value to place
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see org.griphyn.vdl.classes.Derivation
     */
    public long saveAnnotationDerivation(String fqdi, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Annotates a transformation argument with a tuple.
     *
     * @param fqdi is the FQDI to annotate
     * @param formalname is the name of the formal argument to annotoate.
     * @param annotation is the value to place
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see org.griphyn.vdl.classes.Declare
     */
    public long saveAnnotationDeclare(
            String fqdi, String formalname, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Annotates a transformation call with a tuple.
     *
     * @param fqdi is the FQDI to annotate
     * @param index is the number of the call to annotate.
     * @param annotation is the value to place
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see org.griphyn.vdl.classes.Call
     */
    public long saveAnnotationCall(String fqdi, int index, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Annotates a logical filename with a tuple.
     *
     * @param filename is the FQDI to annotate
     * @param annotation is the value to place
     * @param overwrite is a predicate on replace or maintain.
     * @return the insertion id, or -1, if the database was untouched
     * @see org.griphyn.vdl.classes.LFN
     */
    public long saveAnnotationFilename(String filename, Tuple annotation, boolean overwrite)
            throws SQLException, IllegalArgumentException;

    /**
     * Retrieves a specific annotation from an annotatable classes with the specified tuple. This is
     * an interface method to the various class-specific methods.
     *
     * @param primary is the primary object specifier for the class. According to the type, this is
     *     either the FQDI, or the filename.
     * @param secondary is a helper argument for annotations to calls and formal arguments, and
     *     should be null for all other classes. For calls, the argument must be packed into {@link
     *     java.lang.Integer}.
     * @param kind defines the kind/class of object to annotate.
     * @param key is the key to look for.
     * @return null if not found, otherwise the annotation tuple.
     * @see #loadAnnotationTransformation( String, String )
     * @see #loadAnnotationDerivation( String, String )
     * @see #loadAnnotationCall( String, int, String )
     * @see #loadAnnotationDeclare( String, String, String )
     * @see #loadAnnotationFilename( String, String )
     */
    public Tuple loadAnnotation(String primary, Object secondary, int kind, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Obtains the value to a specific key in an annotated transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public Tuple loadAnnotationTransformation(String fqdi, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Obtains the value to a specific key in an annotated derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public Tuple loadAnnotationDerivation(String fqdi, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Obtains the value to a specific key in an annotated formal argument.
     *
     * @param fqdi is the FQDI of the transformation
     * @param farg is the name of the formal argument
     * @param key is the key to search for
     * @return the annotated value, or null if not found
     * @see org.griphyn.vdl.classes.Declare
     */
    public Tuple loadAnnotationDeclare(String fqdi, String farg, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Obtains the value to a specific key for a call statement.
     *
     * @param fqdi is the FQDI of the transformation
     * @param index is the number of the call to annotate.
     * @param key is the key to search for
     * @return the annotated value, or null if not found
     * @see org.griphyn.vdl.classes.Call
     */
    public Tuple loadAnnotationCall(String fqdi, int index, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Obtains the value to a specific key in an annotated filename.
     *
     * @param filename is the name of the file that was annotated.
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.LFN
     */
    public Tuple loadAnnotationFilename(String filename, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Retrieves all annotations from an annotatable classes with the specified tuple. This is an
     * interface method to the various class-specific methods.
     *
     * @param primary is the primary object specifier for the class. According to the type, this is
     *     either the FQDI, or the filename.
     * @param secondary is a helper argument for annotations to calls and formal arguments, and
     *     should be null for all other classes. For calls, the argument must be packed into {@link
     *     java.lang.Integer}.
     * @param kind defines the kind/class of object to annotate.
     * @return null if not found, otherwise the annotation tuple.
     * @see #loadAnnotationTransformation( String )
     * @see #loadAnnotationDerivation( String )
     * @see #loadAnnotationCall( String, int )
     * @see #loadAnnotationDeclare( String, String )
     * @see #loadAnnotationFilename( String )
     */
    public java.util.List loadAnnotation(String primary, Object secondary, int kind)
            throws SQLException, IllegalArgumentException;

    /**
     * Lists all annotations for a transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public java.util.List loadAnnotationTransformation(String fqdi)
            throws SQLException, IllegalArgumentException;

    /**
     * Lists all annotations for a derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public java.util.List loadAnnotationDerivation(String fqdi)
            throws SQLException, IllegalArgumentException;

    /**
     * Lists all annotations for a formal argument.
     *
     * @param fqdi is the FQDI of the transformation
     * @param farg is the name of the formal argument
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Declare
     */
    public java.util.List loadAnnotationDeclare(String fqdi, String farg)
            throws SQLException, IllegalArgumentException;

    /**
     * Lists all annotations for a call statement.
     *
     * @param fqdi is the FQDI of the transformation
     * @param index is the number of the call to annotate.
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Call
     */
    public java.util.List loadAnnotationCall(String fqdi, int index)
            throws SQLException, IllegalArgumentException;

    /**
     * Lists all annotations for a logical filename.
     *
     * @param filename is the logical filename.
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.LFN
     */
    public java.util.List loadAnnotationFilename(String filename)
            throws SQLException, IllegalArgumentException;

    /**
     * Search for LFNs or Definitions that has certain annotations
     *
     * @param kind defines the kind/class of object annotated.
     * @param arg is used only for TR ARG and TR CALL. For the former it is the name of the argument
     *     (String), for the latter the position of the call (Integer).
     * @param tree stores the query tree to query the annotation
     * @return a list of LFNs if search for filenames, otherwise a list of definitions.
     * @exception SQLException if something goes wrong with the database.
     * @see org.griphyn.vdl.annotation.QueryTree
     */
    public abstract java.util.List searchAnnotation(int kind, Object arg, QueryTree tree)
            throws java.sql.SQLException;

    /**
     * Deletes an annotation with the specified key.
     *
     * @param primary is the primary object specifier for the class. According to the type, this is
     *     either the FQDI, or the filename.
     * @param secondary is a helper argument for annotations to calls and formal arguments, and
     *     should be null for all other classes. For calls, the argument must be packed into {@link
     *     java.lang.Integer}.
     * @param kind defines the kind/class of object to annotate.
     * @param key is the annotation key.
     * @return true, if the database was modified, false otherwise.
     * @exception SQLException, if something went wrong during database access.
     */
    public boolean deleteAnnotation(String primary, Object secondary, int kind, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Deletes a specific key in an annotated transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public boolean deleteAnnotationTransformation(String fqdi, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Deletes a specific key in an annotated derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public boolean deleteAnnotationDerivation(String fqdi, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Deletes a specific key in an annotated formal argument.
     *
     * @param fqdi is the FQDI of the transformation
     * @param farg is the name of the formal argument
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Declare
     */
    public boolean deleteAnnotationDeclare(String fqdi, String farg, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Deletes a specific key for a call statement.
     *
     * @param fqdi is the FQDI of the transformation
     * @param index is the number of the call to annotate.
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Call
     */
    public boolean deleteAnnotationCall(String fqdi, int index, String key)
            throws SQLException, IllegalArgumentException;

    /**
     * Deletes a specific key in an annotated filename.
     *
     * @param filename is the name of the file that was annotated.
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.LFN
     */
    public boolean deleteAnnotationFilename(String filename, String key)
            throws SQLException, IllegalArgumentException;
}
