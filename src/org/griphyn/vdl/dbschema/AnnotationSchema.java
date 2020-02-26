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

import edu.isi.pegasus.common.util.Separator;
import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.annotation.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.router.Cache;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class provides basic functionalities to interact with the backend database, such as
 * insertion, deletion, and search. While the main database layout for storing definitions is
 * derived from the "chunk" schema with minor improvements, the database layout of the annotations
 * is shown in the following figure:
 *
 * <p><img src="doc-files/AnnotationSchema-1.png" alt="annotation schema">
 *
 * <p>The central five elements that can receive annotations, all depend on the same sequence
 * generator for their primary key. Their secondary key references the definition in question, or
 * otherwise qualifies the element to annotate. Note that logical filenames can be annotated outside
 * of any definitions.
 *
 * <p>Grouped along the outer edges, five primary data type tables store annotations efficiently.
 * The distinction into separate data types is necessary to enable efficient searches and
 * appropriate operations. Their primary key is also a foreign key referencing the five central
 * elements.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class AnnotationSchema extends DatabaseSchema implements Advanced, Annotation {
    /** Name of the four parameter tables in human readable format. */
    protected static final String[] c_lfn_names = {"ANNO_LFN_I", "ANNO_LFN_O", "ANNO_LFN_B"};

    /** Communication between saveDefinition and deleteDefinition in update mode. */
    protected boolean m_deferDeleteCommit;

    /** An instance of the VDLx XML parser. */
    private org.griphyn.vdl.parser.VDLxParser m_parser;

    /** A cache for definitions to avoid reloading from the database. */
    protected Cache m_cache;

    /**
     * Instantiates an XML parser for VDLx on demand. Since XML parsing and parser instantiation is
     * an expensive business, the reader will only be generated on demand, and only once.
     *
     * @return a valid VDLx parser instance.
     */
    private org.griphyn.vdl.parser.VDLxParser parserInstance() {
        if (this.m_parser == null) {
            // obtain the schema location URL from the schema properties:
            // url is a list of strings representing schema locations. The
            // content exists in pairs, one of the namespace URI, one of the
            // location URL.
            String url = null;
            try {
                ChimeraProperties props = ChimeraProperties.instance();
                url = m_dbschemaprops.getProperty("xml.url", props.getVDLSchemaLocation());
            } catch (IOException e) {
                Logging.instance().log("chunk", 0, "ignored " + e);
            }
            this.m_parser = new org.griphyn.vdl.parser.VDLxParser(url);
        }

        // done
        return this.m_parser;
    }

    /**
     * Default constructor for the "chunk" schema.
     *
     * @param dbDriverName is the database driver name
     */
    public AnnotationSchema(String dbDriverName)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        // load the driver from the properties
        super(dbDriverName, VDC.PROPERTY_PREFIX);
        Logging.instance().log("dbschema", 3, "done with default schema c'tor");

        this.m_cache = this.m_dbdriver.cachingMakesSense() ? new Cache(600) : null;
        this.m_deferDeleteCommit = false;
        this.m_parser = null;

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.definition",
                "INSERT INTO anno_definition(id,type,name,namespace,version,xml) "
                        + "VALUES (?,?,?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.lfn_i", "INSERT INTO anno_lfn_i(did,name) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.lfn_o", "INSERT INTO  anno_lfn_o(did,name) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.lfn_b", "INSERT INTO  anno_lfn_b(did,name) VALUES (?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.updt.definition", "UPDATE  anno_definition SET xml=? WHERE id=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_tr", "INSERT INTO anno_tr(id,did,mkey) VALUES (?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_dv", "INSERT INTO anno_dv(id,did,mkey) VALUES (?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_lfn", "INSERT INTO anno_lfn(id,name,mkey) VALUES (?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_targ", "INSERT INTO anno_targ(id,did,name,mkey) VALUES (?,?,?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_call", "INSERT INTO anno_call(id,did,pos,mkey) VALUES (?,?,?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_bool", "INSERT INTO anno_bool(id,value) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_int", "INSERT INTO anno_int(id,value) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_float", "INSERT INTO anno_float(id,value) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_date", "INSERT INTO anno_date(id,value) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.anno_text", "INSERT INTO anno_text(id,value) VALUES (?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_i", "SELECT distinct did FROM anno_lfn_i WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_o", "SELECT distinct did FROM anno_lfn_o WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_b", "SELECT distinct did FROM anno_lfn_b WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_*",
                "SELECT distinct did FROM anno_lfn_i WHERE name=? UNION "
                        + "SELECT distinct did FROM anno_lfn_o WHERE name=? UNION "
                        + "SELECT distinct did FROM anno_lfn_b WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_*.name",
                "SELECT distinct name FROM anno_lfn_i WHERE did=? UNION "
                        + "SELECT distinct name FROM anno_lfn_o WHERE did=? UNION "
                        + "SELECT distinct name FROM anno_lfn_b WHERE did=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_i.name.ex",
                "SELECT distinct name FROM anno_lfn_i WHERE name LIKE ?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_o.name.ex",
                "SELECT distinct name FROM anno_lfn_o WHERE name LIKE ?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_b.name.ex",
                "SELECT distinct name FROM anno_lfn_b WHERE name LIKE ?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.lfn_*.name.ex",
                "SELECT distinct name FROM anno_lfn_i WHERE name LIKE ? UNION "
                        + "SELECT distinct name FROM anno_lfn_o WHERE name LIKE ? UNION "
                        + "SELECT distinct name FROM anno_lfn_b WHERE name LIKE ?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.big",
                "SELECT id FROM anno_tr WHERE did=? UNION "
                        + "SELECT id FROM anno_dv WHERE did=? UNION "
                        + "SELECT id FROM anno_call WHERE did=? UNION "
                        + "SELECT id FROM anno_targ WHERE did=? UNION "
                        +
                        //	"SELECT id FROM anno_lfn WHERE name IN (" +
                        //	" SELECT distinct name FROM lfn_i WHERE did=? UNION " +
                        //	" SELECT distinct name FROM lfn_o WHERE did=? UNION " +
                        //	" SELECT distinct name FROM lfn_b WHERE did=? )" );
                        "SELECT a.id FROM anno_lfn a, anno_lfn_i i WHERE i.name=a.name AND a.id=? UNION "
                        + "SELECT a.id FROM anno_lfn a, anno_lfn_o o WHERE o.name=a.name AND a.id=? UNION "
                        + "SELECT a.id FROM anno_lfn a, anno_lfn_b b WHERE b.name=a.name AND a.id=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.xml.id", "SELECT xml FROM anno_definition WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.xml",
                "SELECT id,xml FROM anno_definition WHERE type=? AND name=? AND namespace=? AND version=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.id",
                "SELECT id FROM anno_definition WHERE type=? AND name=? AND namespace=? AND version=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.xml", "DELETE FROM anno_definition WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.lfn_i", "DELETE FROM anno_lfn_i WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.lfn_o", "DELETE FROM anno_lfn_o WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.lfn_b", "DELETE FROM anno_lfn_b WHERE did=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_bool", "DELETE FROM anno_bool WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_int", "DELETE FROM anno_int WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_float", "DELETE FROM anno_float WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_date", "DELETE FROM anno_date WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_text", "DELETE FROM anno_text WHERE id=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_tr", "DELETE FROM anno_tr WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_dv", "DELETE FROM anno_dv WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_lfn", "DELETE FROM anno_lfn WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_targ", "DELETE FROM anno_targ WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.anno_call", "DELETE FROM anno_call WHERE id=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_tr", "SELECT id FROM anno_tr WHERE did=? AND mkey=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_dv", "SELECT id FROM anno_dv WHERE did=? AND mkey=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_lfn", "SELECT id FROM anno_lfn WHERE name=? AND mkey=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_targ",
                "SELECT id FROM anno_targ WHERE did=? AND name=? AND mkey=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_call",
                "SELECT id FROM anno_call WHERE did=? AND pos=? AND mkey=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_bool", "SELECT value FROM anno_bool WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_int", "SELECT value FROM anno_int WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_float", "SELECT value FROM anno_float WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_date", "SELECT value FROM anno_date WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_text", "SELECT value FROM anno_text WHERE id=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_tr2", "SELECT id,mkey FROM anno_tr WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_dv2", "SELECT id,mkey FROM anno_dv WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_lfn2", "SELECT id,mkey FROM anno_lfn WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_targ2", "SELECT id,mkey FROM anno_targ WHERE did=? and name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.anno_call2", "SELECT id,mkey FROM anno_call WHERE did=? and pos=?");

        // udpates, take one
        this.m_dbdriver.insertPreparedStatement(
                "stmt.update.anno_tr", "UPDATE anno_tr SET did=? WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.udpate.anno_dv", "UPDATE anno_dv SET did=? WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.update.anno_targ", "UPDATE anno_targ SET did=? WHERE did=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.update.anno_call", "UPDATE anno_call SET did=? WHERE did=?");
    }

    //
    // lower level methods, working directly on specific definitions
    //

    /**
     * Loads a single Definition from the backend database into an Java object. This method does not
     * allow wildcarding!
     *
     * @param namespace namespace, null will be converted into empty string
     * @param name name, null will be converted into empty string
     * @param version version, null will be converted into empty string
     * @param type type of the definition (TR or DV), must not be -1.
     * @return the Definition as specified, or null if not found.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #saveDefinition( Definition, boolean )
     */
    public Definition loadDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        Definition result = null;
        Logging.instance().log("xaction", 1, "START load definition");

        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.xml");
        ps.setInt(i++, type);
        ps.setString(i++, makeNotNull(name));
        ps.setString(i++, makeNotNull(namespace));
        ps.setString(i++, makeNotNull(version));
        Logging.instance().log("chunk", 2, "SELECT xml FROM anno_definition");

        ResultSet rs = ps.executeQuery();
        Logging.instance().log("xaction", 1, "INTER load definition");

        if (rs.next()) {
            MyCallbackHandler cb = new MyCallbackHandler();
            Long lid = new Long(rs.getLong("id"));
            // FIXME: multiple null handlings missing
            parserInstance().parse(new org.xml.sax.InputSource(rs.getCharacterStream("xml")), cb);
            result = cb.getDefinition();

            // add to cache
            if (m_cache != null) m_cache.set(lid, result);
        } else {
            Logging.instance().log("chunk", 0, "Definition not found");
        }

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL load definition");
        return result;
    }

    /**
     * Load a single Definition from the backend database into a Java object by its primary key id.
     * This is an internal helper function.
     *
     * @param id is a long which represent the primary id.
     * @return the Definitions that was matched by the id.
     * @see #loadDefinition( String, String, String, int )
     * @see #saveDefinition( Definition, boolean )
     */
    private Definition loadDefinition(long id) throws SQLException {
        Definition result = null;
        Long lid = new Long(id);
        Logging.instance().log("xaction", 1, "START load definition " + lid);

        // try grabbing from cache
        if (m_cache != null) result = (Definition) m_cache.get(lid);

        if (result == null) {
            // no cache, or not in cache
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.xml.id");
            if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            Logging.instance().log("xaction", 1, "INTER load definitions");

            if (rs.next()) {
                MyCallbackHandler cb = new MyCallbackHandler();

                // FIXME: multiple null handlings missing
                parserInstance()
                        .parse(new org.xml.sax.InputSource(rs.getCharacterStream("xml")), cb);
                result = cb.getDefinition();

                // add to cache
                if (m_cache != null) m_cache.set(lid, result);
            } else {
                Logging.instance().log("chunk", 0, "Definition not found");
            }
            rs.close();
        }

        Logging.instance().log("xaction", 1, "FINAL load definitions");
        return result;
    }

    /**
     * Compiles the name of a DV/TR for log messages.
     *
     * @param d is a definition
     * @return the type plus FQDN of the definition
     */
    private String what(Definition d) {
        StringBuffer result = new StringBuffer();
        switch (d.getType()) {
            case Definition.DERIVATION:
                result.append("DV");
                break;
            case Definition.TRANSFORMATION:
                result.append("TR");
                break;
            default:
                result.append("??");
                break;
        }

        result.append(' ').append(d.shortID());
        return result.toString();
    }

    /**
     * Saves a Definition, that is either a Transformation or Derivation, into the backend database.
     * This method, of course, does not allow wildcarding. The definition has to be completely
     * specified and valid.
     *
     * <p>Please note that updating a definition will remove all the meta- data that was defined for
     * the definition.
     *
     * @param definition is the new Definition to store.
     * @param overwrite true, if existing defitions will be overwritten by new ones with the same
     *     primary (or secondary) key (-set), or false, if a new definition will be rejected on key
     *     matches.
     * @return true, if the backend database was changed, or false, if the definition was not
     *     accepted into the backend.
     * @see org.griphyn.vdl.classes.Definition
     * @see org.griphyn.vdl.classes.Transformation
     * @see org.griphyn.vdl.classes.Derivation
     * @see #loadDefinition( String, String, String, int )
     */
    public boolean saveDefinition(Definition definition, boolean overwrite) throws SQLException {
        Logging.instance().log("chunk", 2, "SAVE DEFINITION started");

        // figure out, if it already exists
        long id = -1;
        try {
            Long temp = getDefinitionId(definition);
            if (temp != null) id = temp.longValue();
        } catch (SQLException e) {
            String cause = e.getMessage();
            Logging.instance()
                    .log(
                            "app",
                            1,
                            "Ignoring SQL exception"
                                    + (cause == null ? "" : ": " + e.getMessage()));
            m_dbdriver.clearWarnings();
        }
        boolean useInsert = (id == -1);

        // if in insertion mode, complain and exit
        if (!useInsert && !overwrite) {
            Logging.instance()
                    .log(
                            "app",
                            0,
                            definition.shortID()
                                    + " already exists (SQL anno_definition.id="
                                    + id
                                    + "), ignoring");
            return false;
        }

        Logging.instance().log("app", 1, "Trying to add " + what(definition));
        PreparedStatement ps =
                m_dbdriver.getPreparedStatement(
                        useInsert ? "stmt.save.definition" : "stmt.updt.definition");

        if (useInsert) {
            // INSERT
            try {
                id = m_dbdriver.sequence1("def_id_seq");
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "In " + definition.shortID() + ": " + e.toString().trim());
                Logging.instance().log("xaction", 1, "START rollback");
                m_dbdriver.cancelPreparedStatement("stmt.save.definition");
                m_dbdriver.rollback();
                Logging.instance().log("xaction", 1, "FINAL rollback");
                return false;
            }

            // add ID explicitely from sequence to insertion
            Logging.instance().log("xaction", 1, "START save definition");
            int i = 1;
            longOrNull(ps, i++, id);

            ps.setInt(i++, definition.getType());
            if (definition.getName() == null)
                throw new SQLException("VDS inconsistency: " + "The name of a definition is null");
            else ps.setString(i++, definition.getName());
            ps.setString(i++, makeNotNull(definition.getNamespace()));
            ps.setString(i++, makeNotNull(definition.getVersion()));
            String xml = definition.toXML((String) null, (String) null);
            ps.setCharacterStream(i++, new StringReader(xml), xml.length());

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO Definition");
            try {
                ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "def_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "In " + definition.shortID() + ": " + e.toString().trim());
                Logging.instance().log("xaction", 1, "START rollback");
                m_dbdriver.cancelPreparedStatement("stmt.save.definition");
                m_dbdriver.rollback();
                Logging.instance().log("xaction", 1, "FINAL rollback");
                return false;
            }
            Logging.instance().log("xaction", 1, "FINAL save definition: ID=" + id);

        } else {
            // UPDATE
            Logging.instance().log("xaction", 1, "START udpate definition");
            int i = 1;
            String xml = definition.toXML((String) null, (String) null);
            ps.setCharacterStream(i++, new StringReader(xml), xml.length());
            longOrNull(ps, i++, id);

            // update prepared values
            Logging.instance().log("chunk", 2, "UPDATE Definition");
            try {
                ps.executeUpdate();
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "In " + definition.shortID() + ": " + e.toString().trim());
                Logging.instance().log("xaction", 1, "START rollback");
                m_dbdriver.cancelPreparedStatement("stmt.updt.definition");
                m_dbdriver.rollback();
                Logging.instance().log("xaction", 1, "FINAL rollback");
                return false;
            }
            Logging.instance().log("xaction", 1, "FINAL update definition: ID=" + id);

            // TODO: Drop all old LFNs
            deleteLFNsForDefinitionId(id);
        }

        // batch save LFNs from Derivations
        if (definition instanceof Derivation) {
            Derivation derivation = (Derivation) definition;
            Set alreadyKnown = new HashSet();
            // ordering MUST MATCH classes.LFN constants!
            PreparedStatement stmt[] = {
                m_dbdriver.getPreparedStatement("stmt.save.lfn_i"),
                m_dbdriver.getPreparedStatement("stmt.save.lfn_o"),
                m_dbdriver.getPreparedStatement("stmt.save.lfn_b")
            };
            int[] count = new int[stmt.length];
            for (int ii = 0; ii < count.length; ++ii) count[ii] = 0;

            for (Iterator j = derivation.iteratePass(); j.hasNext(); ) {
                Value value = ((Pass) j.next()).getValue();
                if (value != null) {
                    switch (value.getContainerType()) {
                        case Value.SCALAR:
                            // check Scalar contents for LFN
                            saveScalar(id, (Scalar) value, alreadyKnown, stmt, count);
                            break;
                        case Value.LIST:
                            // check List for Scalars for LFN
                            for (Iterator k =
                                            ((org.griphyn.vdl.classes.List) value).iterateScalar();
                                    k.hasNext(); ) {
                                saveScalar(id, (Scalar) k.next(), alreadyKnown, stmt, count);
                            }
                            break;
                        default:
                            throw new RuntimeException("unknown container type");
                    }
                }
            }

            for (int ii = 0; ii < stmt.length; ++ii) {
                // anything to do?
                if (count[ii] > 0) {
                    // batch insert
                    Logging.instance()
                            .log(
                                    "chunk",
                                    2,
                                    "BATCH INSERT for " + count[ii] + ' ' + c_lfn_names[ii]);

                    Logging.instance()
                            .log(
                                    "xaction",
                                    1,
                                    "START batch-add " + count[ii] + ' ' + c_lfn_names[ii]);
                    int[] update = stmt[ii].executeBatch();
                    Logging.instance()
                            .log(
                                    "xaction",
                                    1,
                                    "FINAL batch-add " + count[ii] + ' ' + c_lfn_names[ii]);
                }
            }
        }

        // commit the changes
        Logging.instance().log("xaction", 1, "START commit");
        this.m_dbdriver.commit();
        Logging.instance().log("xaction", 1, "FINAL commit");

        // done
        return true;
    }

    /**
     * Saves all logical filenames from a Scalar object. This is a helper function to save a single
     * definition.
     *
     * @param id is the definition id in the DEFINITION table
     * @param scalar is a Scalar instance of which the LFNs are to be saved.
     * @param already is a set of filenames that were already added during this session
     * @param stmt is an array of the ids of the prepared statements for the different tables.
     * @param count count the number of entries in a prepared statement.
     * @see #saveDefinition( Definition, boolean )
     */
    private void saveScalar(
            long id, Scalar scalar, Set already, PreparedStatement[] stmt, int[] count)
            throws SQLException {
        int result = 0;
        for (Iterator i = scalar.iterateLeaf(); i.hasNext(); ) {
            Leaf leaf = (Leaf) i.next();
            // only interested in logical filenames, nothing else
            if (leaf instanceof LFN) {
                LFN lfn = (LFN) leaf;
                String name = lfn.getFilename();

                // already inserted previously?
                if (already.contains(name)) continue;
                else already.add(name);

                // adjust!
                int index = -1;
                int link = lfn.getLink();
                switch (link) {
                    case LFN.INPUT:
                        index = 0;
                        break;
                    case LFN.OUTPUT:
                        index = 1;
                        break;
                    case LFN.INOUT:
                        index = 2;
                        break;
                    default:
                        throw new RuntimeException("Illegal linkage " + link + " for " + name);
                }

                int n = 1;
                if (m_dbdriver.preferString()) stmt[index].setString(n++, Long.toString(id));
                else stmt[index].setLong(n++, id);
                stmt[index].setString(n++, name);
                Logging.instance().log("chunk", 3, "adding LFN " + LFN.toString(link) + ':' + name);
                stmt[index].addBatch();

                count[index]++;
            }
        }
    }

    //
    // higher level methods, allowing for wildcarding unless working on
    // a single Definition.
    //

    /**
     * Obtains the primary key id for a given definition. "Fake" definitions are NOT permissable.
     * This is an internal helper function.
     *
     * @param namespace is the specific namespace, null will be mapped to ""
     * @param name is the specific name, null will be mapped to ""
     * @param version is the specific version, null will be mapped to ""
     * @param type is the type identifier, -1 is not allowed.
     * @return the id of the definition, or null if not found.
     * @see #getDefinitionId( String, String, String, int )
     */
    protected Long getSpecificDefinitionId(String namespace, String name, String version, int type)
            throws SQLException {
        Logging.instance().log("xaction", 1, "START select ID from DEFINITION");
        Long result = null;

        // ps.resetPreparedStatement( "stmt.select.id" );
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.id");
        ps.setInt(i++, type);
        ps.setString(i++, makeNotNull(name));
        ps.setString(i++, makeNotNull(namespace));
        ps.setString(i++, makeNotNull(version));
        Logging.instance().log("chunk", 2, "SELECT id FROM definition");

        ResultSet rs = ps.executeQuery();
        Logging.instance().log("xaction", 1, "INTER select ID from DEFINITION");
        if (rs.next()) result = new Long(rs.getLong(1));
        else Logging.instance().log("chunk", 0, "Definition not found");

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select ID from DEFINITION");
        return result;
    }

    /**
     * Obtains the primary key id for a given definition. "Fake" definitions are permissable. This
     * is an internal helper function.
     *
     * @param d is a definition specification.
     * @return the id of the definition, or null if not found.
     * @see #getSpecificDefinitionId( String, String, String, int )
     * @see #getDefinitionId( String, String, String, int )
     */
    protected Long getDefinitionId(Definition d) throws SQLException {
        return getSpecificDefinitionId(d.getNamespace(), d.getName(), d.getVersion(), d.getType());
    }

    /**
     * Obtains the list of primary key ids for a matching definitions. This method allows for
     * wildcards in the usual fashion. Use null for strings as wildcards, and -1 for the type
     * wildcard. This method may return an empty list, but it will not return null. This is an
     * internal helper function.
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type definition type (TR or DV)
     * @return a possibly empty list containing all matching definition ids as Longs.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #getDefinitionId( Definition )
     */
    protected java.util.List getDefinitionId(
            String namespace, String name, String version, int type) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START select IDs from DEFINITION");

        java.util.List select = new ArrayList(1);
        select.add(new String("distinct id"));

        java.util.Map where = new TreeMap();
        if (type != -1) where.put("type", Integer.toString(type));
        if (namespace != null) where.put("namespace", namespace);
        if (name != null) where.put("name", name);
        if (version != null) where.put("version", version);

        ResultSet rs = m_dbdriver.select(select, "anno_definition", where, null);
        while (rs.next()) result.add(new Long(rs.getLong("id")));

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select IDs from DEFINITION");
        return result;
    }

    /**
     * Obtains the list of primary key ids for a matching definitions. This method allows for
     * wildcards in the usual fashion. Use null for strings as wildcards, and -1 for the type
     * wildcard. It also allows special characters '%' and '_' in strings. This method may return an
     * empty list, but it will not return null. This is an internal helper function.
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type definition type (TR or DV)
     * @return a possibly empty list containing all matching definition ids as Longs.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #getDefinitionId( Definition )
     */
    protected java.util.List getDefinitionIdEx(
            String namespace, String name, String version, int type) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START select IDs from DEFINITION");

        java.util.List select = new ArrayList(1);
        select.add(new String("distinct id"));

        java.util.Map where = new TreeMap();
        java.util.Map operator = new TreeMap();

        if (type != -1) where.put("type", Integer.toString(type));

        if (namespace != null) {
            where.put("namespace", namespace);
            operator.put("namespace", "LIKE");
        }
        if (name != null) {
            where.put("name", name);
            operator.put("name", "LIKE");
        }
        if (version != null) {
            where.put("version", version);
            operator.put("version", "LIKE");
        }

        ResultSet rs = m_dbdriver.select(select, "anno_definition", where, operator, null);
        while (rs.next()) result.add(new Long(rs.getLong("id")));

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select IDs from DEFINITION");
        return result;
    }

    /**
     * Search the database for the existence of a definition.
     *
     * @param definition the definition object to search for
     * @return true, if the definition exists, false if not found
     */
    public boolean containsDefinition(Definition definition) throws SQLException {
        boolean result = false;
        try {
            result = (getDefinitionId(definition) != null);
        } catch (SQLException sql) {
            // ignore
            this.m_dbdriver.clearWarnings();
        }
        return result;
    }

    /**
     * Deletes the joined annotations when a definition is being deleted.
     *
     * @param id is the definition id to remove
     * @return list of all annotation ids that were removed
     */
    private java.util.List deleteAnnotationFromDefinition(long id) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        ArrayList idlist = new ArrayList();
        boolean preferString = m_dbdriver.preferString();

        //
        // part 1: assemble all affected annotation ids
        //
        // fnlist := collect list of filenames WHERE id=$id
        // annolist := SELECT distinct id FROM anno_$rest WHERE did=$id
        // annolist += SELECT distinct id FROM anno_lfn WHERE name IN $fnlist

        Logging.instance().log("xaction", 1, "START *huge* union");
        ps = this.m_dbdriver.getPreparedStatement("stmt.select.big");
        for (int i = 1; i <= 7; ++i) {
            if (preferString) ps.setString(i, Long.toString(id));
            else ps.setLong(i, id);
        }

        rs = ps.executeQuery();
        while (rs.next()) {
            idlist.add(new Long(rs.getLong(1)));
        }
        rs.close();
        Logging.instance().log("xaction", 1, "FINAL *huge* union");

        //
        // part 2: remove all affected annotations
        //
        // DELETE anno_$type WHERE id IN $annolist
        // DELETE anno_$rest WHERE id IN $annolist

        Logging.instance().log("xaction", 1, "START delete annotations");

        // list of all statements we need to access
        PreparedStatement list[] = {
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_bool"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_int"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_float"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_date"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_text"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_tr"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_dv"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_lfn"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_targ"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_call")
        };

        // prepare and batch all statements
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            id = ((Long) i.next()).longValue();
            for (int j = 0; j < list.length; ++j) {
                if (preferString) list[j].setString(1, Long.toString(id));
                else list[j].setLong(1, id);
                list[j].addBatch();
            }
        }

        // run all batches
        Logging.instance().log("xaction", 1, "INTER delete annotations");
        for (int j = 0; j < list.length; ++j) {
            int[] status = new int[idlist.size()];
            try {
                status = list[j].executeBatch();
            } catch (NullPointerException npe) {
                Logging.instance().log("app", 1, "tripped over NPE, ignoring!");
            }
        }

        Logging.instance().log("xaction", 1, "FINAL delete annotation");
        if (!m_deferDeleteCommit) m_dbdriver.commit();
        return idlist;
    }

    /**
     * Deletes all logical filenames that are associated with a particular definition. Note that
     * this method does not commit the changes, as it is supposed to be called from other methods.
     *
     * @param did is the definition id to remove LFNs for
     * @return number of rows affected, may be zero.
     */
    private int deleteLFNsForDefinitionId(long did) throws SQLException {
        int status, result = 0;
        PreparedStatement ps = null;
        boolean preferString = m_dbdriver.preferString();

        Logging.instance().log("xaction", 1, "START DELETE FROM lfn_i");
        ps = m_dbdriver.getPreparedStatement("stmt.delete.lfn_i");
        if (preferString) ps.setString(1, Long.toString(did));
        else ps.setLong(1, did);
        result = ps.executeUpdate();
        Logging.instance().log("xaction", 1, "FINAL DELETE FROM lfn_i: " + result);

        Logging.instance().log("xaction", 1, "START DELETE FROM lfn_o");
        ps = m_dbdriver.getPreparedStatement("stmt.delete.lfn_o");
        if (preferString) ps.setString(1, Long.toString(did));
        else ps.setLong(1, did);
        status = ps.executeUpdate();
        result += status;
        Logging.instance().log("xaction", 1, "FINAL DELETE FROM lfn_o: " + status);

        Logging.instance().log("xaction", 1, "START DELETE FROM lfn_b");
        ps = m_dbdriver.getPreparedStatement("stmt.delete.lfn_b");
        if (preferString) ps.setString(1, Long.toString(did));
        else ps.setLong(1, did);
        status = ps.executeUpdate();
        result += status;
        Logging.instance().log("xaction", 1, "FINAL DELETE FROM lfn_b: " + status);

        return result;
    }

    /**
     * Delete a specific Definition objects from the database. No wildcard matching will be done.
     * "Fake" definitions are permissable, meaning it just has the secondary key triple.
     *
     * @param definition is the definition specification to delete
     * @return true is something was deleted, false if non existent.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public boolean deleteDefinition(Definition definition) throws SQLException {
        int result = 0;
        PreparedStatement ps = null;

        //
        // TODO: turn into a stored procedure call
        //
        Logging.instance().log("xaction", 1, "START delete definition");
        Long defId = getDefinitionId(definition);
        boolean preferString = m_dbdriver.preferString();
        if (defId != null) {
            long id = defId.longValue();
            deleteAnnotationFromDefinition(id);
            deleteLFNsForDefinitionId(id);

            Logging.instance().log("xaction", 1, "START DELETE FROM definition");
            ps = m_dbdriver.getPreparedStatement("stmt.delete.xml");
            if (preferString) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            result = ps.executeUpdate();
            Logging.instance().log("xaction", 1, "FINAL DELETE FROM definition: " + result);

            if (!m_deferDeleteCommit) m_dbdriver.commit();
        }

        Logging.instance().log("xaction", 1, "FINAL delete definition");
        return (result != 0);
    }

    /**
     * Delete Definition objects from the database. This method allows for wildcards in the usual
     * fashion. Use null for strings as wildcards, and -1 for the type wildcard. For efficiency
     * reasons, this method will return an empty list.
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type definition type (TR or DV)
     * @return a list containing all Definitions that were deleted
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START delete definitions");

        java.util.List idlist = getDefinitionId(namespace, name, version, type);
        if (idlist.size() == 0) return result;
        // postcondition: contains all IDs, count(id)>0, to be deleted

        // save old values
        if (!m_deferDeleteCommit) {
            // we come from saveDefinition, thus we won't need saved values
            for (Iterator i = idlist.iterator(); i.hasNext(); ) {
                Definition d = loadDefinition(((Long) i.next()).longValue());
                if (d != null) result.add(d);
            }
        }

        // remove all affected annoations by walking through them
        // yuk, this is probably extremely expensive
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            long id = ((Long) i.next()).longValue();
            deleteAnnotationFromDefinition(id);
        }

        // list of all statements we need to access
        PreparedStatement ps[] = {
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_i"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_o"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_b"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.xml")
        };

        // prepare and batch all statements
        boolean preferString = m_dbdriver.preferString();
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            long id = ((Long) i.next()).longValue();
            for (int j = 0; j < ps.length; ++j) {
                if (preferString) ps[j].setString(1, Long.toString(id));
                else ps[j].setLong(1, id);
                ps[j].addBatch();
            }
        }

        // run all batches
        Logging.instance().log("xaction", 1, "INTER delete definitions");
        for (int j = 0; j < ps.length; ++j) {
            int[] status = new int[idlist.size()];
            try {
                status = ps[j].executeBatch();
            } catch (NullPointerException npe) {
                Logging.instance().log("app", 1, "tripped over NPE, ignoring!");
            }
        }

        Logging.instance().log("xaction", 1, "FINAL delete definitions");
        if (!m_deferDeleteCommit) m_dbdriver.commit();
        return result;
    }

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type type of definition (TR/DV, or both)
     * @return a list of definitions
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List searchDefinition(String namespace, String name, String version, int type)
            throws SQLException {
        java.util.List idlist = getDefinitionId(namespace, name, version, type);

        // TODO: make this a batch or sproc
        java.util.List result = new ArrayList();
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            Definition d = loadDefinition(((Long) i.next()).longValue());
            if (d != null) result.add(d);
        }
        return result;
    }

    /**
     * Searches the database for all derivations that contain a certain LFN. The linkage is an
     * additional constraint. This method does not allow jokers.
     *
     * @param lfn the LFN name
     * @param link the linkage type of the LFN
     * @return a list of Definition items that match the criterion.
     * @see org.griphyn.vdl.classes.LFN#NONE
     * @see org.griphyn.vdl.classes.LFN#INPUT
     * @see org.griphyn.vdl.classes.LFN#OUTPUT
     * @see org.griphyn.vdl.classes.LFN#INOUT
     */
    public java.util.List searchFilename(String lfn, int link) throws SQLException {
        if (lfn == null) throw new NullPointerException("You must query for a filename");
        Logging.instance().log("xaction", 1, "START select LFNs");
        PreparedStatement ps = null;

        if (link == -1) {
            // wildcard match
            ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_*");
            for (int ii = 0; ii < c_lfn_names.length; ++ii) ps.setString(ii + 1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct id FROM anno_lfn_*" + " WHERE name='" + lfn + "'");
        } else if (LFN.isInRange(link)) {
            // known linkage, one table only

            // ordering MUST MATCH classes.LFN constants!
            switch (link) {
                case LFN.NONE:
                    throw new RuntimeException("The linkage \"none\" is not permitted");
                    // break;
                case LFN.INPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_i");
                    break;
                case LFN.OUTPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_o");
                    break;
                case LFN.INOUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_b");
                    break;
            }
            ;

            ps.setString(1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct id FROM "
                                    + c_lfn_names[link - 1]
                                    + " WHERE name='"
                                    + lfn
                                    + "'");
        } else {
            throw new RuntimeException("The linkage " + link + " is not permitted");
        }

        ResultSet rs = ps.executeQuery();

        // TODO: make this a batch or sproc
        java.util.List result = new ArrayList();
        while (rs.next()) {
            Definition d = loadDefinition(rs.getLong(1));
            if (d != null) result.add(d);
        }

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select LFNs");
        return result;
    }

    //
    //
    // annotations
    //
    //

    /**
     * Retrieves the annotation id of a given transformation.
     *
     * @param did is the definition id of the transformation
     * @param key is the key to search for.
     * @return -1 if not found, or the correction annotation id.
     * @exception SQLException if some database operation fails.
     */
    private long getAnnotationIdTransformation(long did, String key) throws SQLException {
        long result = -1;

        Logging.instance().log("xaction", 1, "START select anno_tr");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_tr");
        if (m_dbdriver.preferString()) ps.setString(i++, Long.toString(did));
        else ps.setLong(i++, did);
        ps.setString(i++, makeNotNull(key));

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_tr");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_tr.id = " + result);
        return result;
    }

    /**
     * Retrieves the annotation id of a given derivation id.
     *
     * @param did is the definition id of the derivation
     * @param key is the key to search for.
     * @return -1 if not found, or the correction annotation id.
     * @exception SQLException if some database operation fails.
     */
    private long getAnnotationIdDerivation(long did, String key) throws SQLException {
        long result = -1;

        Logging.instance().log("xaction", 1, "START select anno_dv");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_dv");
        if (m_dbdriver.preferString()) ps.setString(i++, Long.toString(did));
        else ps.setLong(i++, did);
        ps.setString(i++, makeNotNull(key));

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_dv");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_dv.id = " + result);
        return result;
    }

    /**
     * Retrieves the annotation id of a given transformation formal argument.
     *
     * @param did is the definition id of the transformation
     * @param farg is the formal argument name
     * @param key is the key to search for.
     * @return -1 if not found, or the correction annotation id.
     * @exception SQLException if some database operation fails.
     */
    private long getAnnotationIdDeclare(long did, String farg, String key) throws SQLException {
        long result = -1;

        Logging.instance().log("xaction", 1, "START select anno_targ");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_targ");
        if (m_dbdriver.preferString()) ps.setString(i++, Long.toString(did));
        else ps.setLong(i++, did);
        ps.setString(i++, makeNotNull(farg));
        ps.setString(i++, makeNotNull(key));

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_targ");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_targ.id = " + result);
        return result;
    }

    /**
     * Retrieves the annotation id of a given call within a transformation.
     *
     * @param did is the definition id of the transformation
     * @param pos is the position of the call statement
     * @param key is the key to search for.
     * @return -1 if not found, or the correction annotation id.
     * @exception SQLException if some database operation fails.
     */
    private long getAnnotationIdCall(long did, int pos, String key) throws SQLException {
        long result = -1;

        Logging.instance().log("xaction", 1, "START select anno_call");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_call");
        if (m_dbdriver.preferString()) {
            ps.setString(i++, Long.toString(did));
            ps.setString(i++, Integer.toString(pos));
        } else {
            ps.setLong(i++, did);
            ps.setInt(i++, pos);
        }
        ps.setString(i++, makeNotNull(key));

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_call");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_call.id = " + result);
        return result;
    }

    /**
     * Retrieves the annotation id of a given logical filename.
     *
     * @param lfn is the logical filename.
     * @param key is the key to search for.
     * @return -1 if not found, or the correction annotation id.
     * @exception SQLException if some database operation fails.
     */
    private long getAnnotationIdFilename(String lfn, String key) throws SQLException {
        long result = -1;

        Logging.instance().log("xaction", 1, "START select anno_lfn");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_lfn");
        ps.setString(i++, makeNotNull(lfn));
        ps.setString(i++, makeNotNull(key));

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_lfn");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) result = rs.getLong(1);
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_lfn.id = " + result);
        return result;
    }

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
            throws SQLException, IllegalArgumentException {
        boolean result = true;
        switch (kind) {
            case CLASS_TRANSFORMATION:
                result = deleteAnnotationTransformation(primary, key);
                break;
            case CLASS_DERIVATION:
                result = deleteAnnotationDerivation(primary, key);
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                result = deleteAnnotationCall(primary, ((Integer) secondary).intValue(), key);
                break;
            case CLASS_DECLARE:
                // may throw ClassCastException
                result = deleteAnnotationDeclare(primary, ((String) secondary), key);
                break;
            case CLASS_FILENAME:
                result = deleteAnnotationFilename(primary, key);
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        return result;
    }

    /**
     * Deletes an annotation value with the specified annotation id.
     *
     * @param id is the annotation id for which to delete
     * @return true, if the database was modified, false otherwise.
     * @exception SQLException, if something went wrong during database access.
     */
    private boolean deleteAnnotationValue(long id) throws SQLException {
        PreparedStatement list[] = {
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_bool"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_int"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_float"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_date"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.anno_text")
        };

        boolean result = true;

        // prepare and batch all statements
        for (int j = 0; j < list.length; ++j) {
            if (m_dbdriver.preferString()) list[j].setString(1, Long.toString(id));
            else list[j].setLong(1, id);
            list[j].addBatch();
        }

        // run all batches
        Logging.instance().log("xaction", 1, "INTER delete annotations");
        for (int j = 0; j < list.length; ++j) {
            int[] status = new int[1];
            try {
                status = list[j].executeBatch();
                result = (result && (status[0] != 0));
            } catch (NullPointerException npe) {
                Logging.instance().log("app", 1, "tripped over NPE, ignoring!");
            }
        }

        Logging.instance().log("xaction", 1, "FINAL delete annotation");
        if (!m_deferDeleteCommit) m_dbdriver.commit();
        return result;
    }

    /**
     * Deletes an annotation in a type table with the specified annotation id. The table is
     * determined from the type of the annotational tuple.
     *
     * @param id is the annotation id for which to delete
     * @param kind is the class of object.
     * @return true, if the database was modified, false otherwise.
     * @exception SQLException, if something went wrong during database access.
     */
    private boolean deleteAnnotationKey(long id, int kind) throws SQLException {
        Logging.instance().log("xaction", 1, "START delete anno_<key>");

        PreparedStatement ps = null;
        switch (kind) {
            case CLASS_FILENAME:
                ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_lfn");
                break;
            case CLASS_TRANSFORMATION:
                ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_tr");
                break;
            case CLASS_DERIVATION:
                ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_dv");
                break;
            case CLASS_DECLARE:
                ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_targ");
                break;
            case CLASS_CALL:
                ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_call");
                break;
            default:
                throw new SQLException("Don't know the class of object");
        }

        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "DELETE FROM anno_<key>");
        int rc = ps.executeUpdate();
        Logging.instance().log("xaction", 1, "FINAL delete anno_<key>");
        boolean ret = deleteAnnotationValue(id);
        return (rc != 0 && ret);
    }

    /**
     * Deletes an annotation in a type table with the specified annotation id. The table is
     * determined from the type of the annotational tuple.
     *
     * @param id is the annotation id for which to delete
     * @param annotation is the annotation which determines the type
     * @return true, if the database was modified, false otherwise.
     * @exception SQLException, if something went wrong during database access.
     */
    private boolean deleteAnnotationValue(long id, Tuple annotation) throws SQLException {
        Logging.instance().log("xaction", 1, "START delete anno_<value>");

        PreparedStatement ps = null;
        if (annotation instanceof TupleBoolean)
            ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_bool");
        else if (annotation instanceof TupleDate)
            ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_date");
        else if (annotation instanceof TupleFloat)
            ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_float");
        else if (annotation instanceof TupleInteger)
            ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_int");
        else if (annotation instanceof TupleString)
            ps = m_dbdriver.getPreparedStatement("stmt.delete.anno_text");
        else throw new SQLException("Don't know the tuple type");

        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "DELETE FROM anno_<value>");
        int rc = ps.executeUpdate();
        Logging.instance().log("xaction", 1, "FINAL delete anno_<value>");
        return (rc != 0);
    }

    /**
     * Deletes a specific key in an annotated transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public boolean deleteAnnotationTransformation(String fqdi, String key)
            throws SQLException, IllegalArgumentException {
        int kind = CLASS_TRANSFORMATION;

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_tr id
        long id = getAnnotationIdTransformation(did.longValue(), key);

        // no such key, if the id is -1, handled by finalizer
        return deleteAnnotationKey(id, kind);
    }

    /**
     * Deletes a specific key in an annotated derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public boolean deleteAnnotationDerivation(String fqdi, String key)
            throws SQLException, IllegalArgumentException {
        int kind = CLASS_DERIVATION;

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.DERIVATION);
        if (did == null) throw new SQLException("Unknown DV " + fqdi);

        // obtain possible existing anno_dv id
        long id = getAnnotationIdDerivation(did.longValue(), key);

        // no such key, if the id does not exist
        return deleteAnnotationKey(id, kind);
    }

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
            throws SQLException, IllegalArgumentException {
        int kind = CLASS_DECLARE;

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_call id
        long id = getAnnotationIdDeclare(did.longValue(), farg, key);

        // no such key, if the id does not exist
        return deleteAnnotationKey(id, kind);
    }

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
            throws SQLException, IllegalArgumentException {
        int kind = CLASS_CALL;

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_call id
        long id = getAnnotationIdCall(did.longValue(), index, key);

        // no such key, if the id does not exist
        return deleteAnnotationKey(id, kind);
    }

    /**
     * Deletes a specific key in an annotated filename.
     *
     * @param filename is the name of the file that was annotated.
     * @param key is the key to search for
     * @return true, if the database was modified, false otherwise.
     * @see org.griphyn.vdl.classes.LFN
     */
    public boolean deleteAnnotationFilename(String filename, String key)
            throws SQLException, IllegalArgumentException {
        int kind = CLASS_FILENAME;

        // obtain possible existing anno_lfn id
        long id = getAnnotationIdFilename(filename, key);

        // no such key, if the id does not exist
        return deleteAnnotationKey(id, kind);
    }

    /**
     * Inserts a tuple into the correct type-table for annotations.
     *
     * @param id is the annotation id for which to insert.
     * @param annotation is the annotation to insert. Only the type and value will be taken, as the
     *     key was inserted elsewhere.
     * @return true, if the database was modified, false otherwise.
     * @exception SQLException if something during the database access went awry.
     */
    private boolean saveAnnotationValue(long id, Tuple annotation) throws SQLException {
        Logging.instance().log("xaction", 1, "START save anno_<value>");

        PreparedStatement ps = null;
        if (annotation instanceof TupleBoolean) {
            ps = m_dbdriver.getPreparedStatement("stmt.save.anno_bool");
            ps.setBoolean(2, ((Boolean) annotation.getValue()).booleanValue());
        } else if (annotation instanceof TupleDate) {
            ps = m_dbdriver.getPreparedStatement("stmt.save.anno_date");
            ps.setTimestamp(2, ((Timestamp) annotation.getValue()));
        } else if (annotation instanceof TupleFloat) {
            ps = m_dbdriver.getPreparedStatement("stmt.save.anno_float");
            ps.setDouble(2, ((Double) annotation.getValue()).doubleValue());
        } else if (annotation instanceof TupleInteger) {
            ps = m_dbdriver.getPreparedStatement("stmt.save.anno_int");
            ps.setLong(2, ((Long) annotation.getValue()).longValue());
        } else if (annotation instanceof TupleString) {
            ps = m_dbdriver.getPreparedStatement("stmt.save.anno_text");
            // ps.setString( 2, ((String) annotation.getValue()) );
            String value = (String) annotation.getValue();
            ps.setCharacterStream(2, new StringReader(value), value.length());
        } else throw new SQLException("Don't know the tuple type");

        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "INSERT INTO anno_<value>");
        int rc = ps.executeUpdate();
        Logging.instance().log("xaction", 1, "FINAL save anno_<value>");
        return (rc != 0);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_tr id
        long id = getAnnotationIdTransformation(did.longValue(), annotation.getKey());

        // insert into anno_tr with new id, if id does not exist
        if (id == -1) {
            // obtain new id
            id = m_dbdriver.sequence1("anno_id_seq");

            Logging.instance().log("xaction", 1, "START save anno_tr");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.anno_tr");

            int i = 1;
            longOrNull(ps, i++, id);

            if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
            else ps.setLong(i++, did.longValue());
            ps.setString(i++, makeNotNull(annotation.getKey()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO anno_tr");
            try {
                int rc = ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "anno_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "While inserting into anno_tr: " + e.toString().trim());
                // rollback in saveInvocation()
                m_dbdriver.cancelPreparedStatement("stmt.save.anno_tr");
                throw e; // re-throw
            }
            Logging.instance().log("xaction", 1, "FINAL save anno_tr: ID=" + id);
        } else {
            // id does exist, nothing to do in anno_tr
        }

        // delete before insert if overwrite mode
        if (overwrite) deleteAnnotationValue(id, annotation);
        return (saveAnnotationValue(id, annotation) ? id : -1);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.DERIVATION);
        if (did == null) throw new SQLException("Unknown DV " + fqdi);

        // obtain possible existing anno_dv id
        long id = getAnnotationIdDerivation(did.longValue(), annotation.getKey());

        // insert into anno_dv with new id, if id does not exist
        if (id == -1) {
            // obtain new id
            id = m_dbdriver.sequence1("anno_id_seq");

            Logging.instance().log("xaction", 1, "START save anno_dv");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.anno_dv");
            int i = 1;
            longOrNull(ps, i++, id);
            if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
            else ps.setLong(i++, did.longValue());
            ps.setString(i++, makeNotNull(annotation.getKey()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO anno_dv");
            try {
                int rc = ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "anno_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "While inserting into anno_tr: " + e.toString().trim());
                // rollback in saveInvocation()
                m_dbdriver.cancelPreparedStatement("stmt.save.anno_tr");
                throw e; // re-throw
            }
            Logging.instance().log("xaction", 1, "FINAL save anno_dv: ID=" + id);
        } else {
            // id does exist, nothing to do in anno_tr
        }

        // delete before insert if overwrite mode
        if (overwrite) deleteAnnotationValue(id, annotation);
        return (saveAnnotationValue(id, annotation) ? id : -1);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        Transformation tr = (Transformation) loadDefinition(did.longValue());
        boolean found = false;
        for (Iterator i = tr.iterateDeclare(); i.hasNext(); ) {
            String arg = ((Declare) i.next()).getName();
            if (arg.equals(formalname)) {
                found = true;
                break;
            }
        }

        if (!found) throw new SQLException("Invalid argument " + formalname + " for TR " + fqdi);

        // obtain possible existing anno_farg id
        long id = getAnnotationIdDeclare(did.longValue(), formalname, annotation.getKey());

        // insert into anno_dv with new id, if id does not exist
        if (id == -1) {
            // obtain new id
            id = m_dbdriver.sequence1("anno_id_seq");

            Logging.instance().log("xaction", 1, "START save anno_targ");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.anno_targ");
            int i = 1;
            longOrNull(ps, i++, id);
            if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
            else ps.setLong(i++, did.longValue());
            ps.setString(i++, makeNotNull(formalname));
            ps.setString(i++, makeNotNull(annotation.getKey()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO anno_targ");
            try {
                int rc = ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "anno_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "While inserting into anno_targ: " + e.toString().trim());
                // rollback in saveInvocation()
                m_dbdriver.cancelPreparedStatement("stmt.save.anno_targ");
                throw e; // re-throw
            }
            Logging.instance().log("xaction", 1, "FINAL save anno_targ: ID=" + id);
        } else {
            // id does exist, nothing to do in anno_targ
        }

        // delete before insert if overwrite mode
        if (overwrite) deleteAnnotationValue(id, annotation);
        return (saveAnnotationValue(id, annotation) ? id : -1);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        Transformation tr = (Transformation) loadDefinition(did.longValue());
        if (index <= 0 || tr.getCallCount() < index)
            throw new SQLException("Invalid position " + index + " for TR " + fqdi);

        // obtain possible existing anno_call id
        long id = getAnnotationIdCall(did.longValue(), index, annotation.getKey());

        // insert into anno_dv with new id, if id does not exist
        if (id == -1) {
            // obtain new id
            id = m_dbdriver.sequence1("anno_id_seq");

            Logging.instance().log("xaction", 1, "START save anno_call");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.anno_call");
            int i = 1;
            longOrNull(ps, i++, id);

            if (m_dbdriver.preferString()) {
                ps.setString(i++, did.toString());
                ps.setString(i++, Integer.toString(index));
            } else {
                ps.setLong(i++, did.longValue());
                ps.setInt(i++, index);
            }
            ps.setString(i++, makeNotNull(annotation.getKey()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO anno_call");
            try {
                int rc = ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "anno_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "While inserting into anno_call: " + e.toString().trim());
                // rollback in saveInvocation()
                m_dbdriver.cancelPreparedStatement("stmt.save.anno_call");
                throw e; // re-throw
            }
            Logging.instance().log("xaction", 1, "FINAL save anno_targ: ID=" + id);
        } else {
            // id does exist, nothing to do in anno_targ
        }

        // delete before insert if overwrite mode
        if (overwrite) deleteAnnotationValue(id, annotation);
        return (saveAnnotationValue(id, annotation) ? id : -1);
    }

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
            throws SQLException, IllegalArgumentException {
        // obtain possible existing anno_lfn id
        long id = getAnnotationIdFilename(filename, annotation.getKey());

        // insert into anno_dv with new id, if id does not exist
        if (id == -1) {
            // obtain new id
            id = m_dbdriver.sequence1("anno_id_seq");

            Logging.instance().log("xaction", 1, "START save anno_lfn");
            PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.anno_lfn");
            int i = 1;
            longOrNull(ps, i++, id);
            ps.setString(i++, makeNotNull(filename));
            ps.setString(i++, makeNotNull(annotation.getKey()));

            // save prepared values
            Logging.instance().log("chunk", 2, "INSERT INTO anno_lfn");
            try {
                int rc = ps.executeUpdate();
                if (id == -1) id = m_dbdriver.sequence2(ps, "anno_id_seq", 1);
            } catch (SQLException e) {
                Logging.instance()
                        .log("app", 0, "While inserting into anno_lfn: " + e.toString().trim());
                // rollback in saveInvocation()
                m_dbdriver.cancelPreparedStatement("stmt.save.anno_lfn");
                throw e; // re-throw
            }
            Logging.instance().log("xaction", 1, "FINAL save anno_lfn: ID=" + id);
        } else {
            // id does exist, nothing to do in anno_targ
        }

        // delete before insert if overwrite mode
        if (overwrite) deleteAnnotationValue(id, annotation);
        return (saveAnnotationValue(id, annotation) ? id : -1);
    }

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
            throws SQLException, IllegalArgumentException {
        long result = -1;
        switch (kind) {
            case CLASS_TRANSFORMATION:
                result = saveAnnotationTransformation(primary, annotation, overwrite);
                break;
            case CLASS_DERIVATION:
                result = saveAnnotationDerivation(primary, annotation, overwrite);
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                result =
                        saveAnnotationCall(
                                primary, ((Integer) secondary).intValue(), annotation, overwrite);
                break;
            case CLASS_DECLARE:
                // may throw ClassCastException
                result =
                        saveAnnotationDeclare(primary, ((String) secondary), annotation, overwrite);
                break;
            case CLASS_FILENAME:
                result = saveAnnotationFilename(primary, annotation, overwrite);
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }
        if (result != -1) if (!m_deferDeleteCommit) m_dbdriver.commit();

        return result;
    }

    /**
     * Obtains the value at a specific id from the boolean annotations.
     *
     * @param id is the annotation id
     * @param key is used to create the tuple
     * @return null if not found, or a valid tuple otherwise.
     */
    private TupleBoolean loadAnnotationBoolean(long id, String key) throws SQLException {
        TupleBoolean result = null;

        Logging.instance().log("xaction", 1, "START select anno_bool");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_bool");
        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "SELECT value FROM anno_bool");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            boolean value = rs.getBoolean(1);
            result = new TupleBoolean(key, value);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_bool");
        return result;
    }

    /**
     * Obtains the value at a specific id from the integer annotations.
     *
     * @param id is the annotation id
     * @param key is used to create the tuple
     * @return null if not found, or a valid tuple otherwise.
     */
    private TupleInteger loadAnnotationInteger(long id, String key) throws SQLException {
        TupleInteger result = null;

        Logging.instance().log("xaction", 1, "START select anno_int");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_int");
        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "SELECT value FROM anno_int");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            long value = rs.getLong(1);
            result = new TupleInteger(key, value);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_int");
        return result;
    }

    /**
     * Obtains the value at a specific id from the float annotations.
     *
     * @param id is the annotation id
     * @param key is used to create the tuple
     * @return null if not found, or a valid tuple otherwise.
     */
    private TupleFloat loadAnnotationFloat(long id, String key) throws SQLException {
        TupleFloat result = null;

        Logging.instance().log("xaction", 1, "START select anno_float");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_float");
        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "SELECT value FROM anno_float");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            double value = rs.getDouble(1);
            result = new TupleFloat(key, value);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_float");
        return result;
    }

    /**
     * Obtains the value at a specific id from the date annotations.
     *
     * @param id is the annotation id
     * @param key is used to create the tuple
     * @return null if not found, or a valid tuple otherwise.
     */
    private TupleDate loadAnnotationDate(long id, String key) throws SQLException {
        TupleDate result = null;

        Logging.instance().log("xaction", 1, "START select anno_date");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_date");
        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "SELECT value FROM anno_date");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            java.sql.Timestamp value = rs.getTimestamp(1);
            result = new TupleDate(key, value);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_date");
        return result;
    }

    /**
     * Obtains the value at a specific id from the string annotations.
     *
     * @param id is the annotation id
     * @param key is used to create the tuple
     * @return null if not found, or a valid tuple otherwise.
     */
    private TupleString loadAnnotationString(long id, String key) throws SQLException {
        TupleString result = null;

        Logging.instance().log("xaction", 1, "START select anno_text");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_text");
        if (m_dbdriver.preferString()) ps.setString(1, Long.toString(id));
        else ps.setLong(1, id);

        Logging.instance().log("chunk", 2, "SELECT value FROM anno_text");
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            Reader r = rs.getCharacterStream(1);
            StringBuffer temp = new StringBuffer(128);
            try {
                int ch;
                while ((ch = r.read()) >= 0) temp.append((char) ch);
            } catch (IOException ioe) {
                throw new SQLException(ioe.getMessage());
            }
            result = new TupleString(key, temp.toString());
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_text");
        return result;
    }

    /**
     * Tries to retrieve a tuple value from its annotation id by walking over all tables, most
     * likely one first. This is an internal function helping to keep common code common.
     *
     * @param id is the annotation id to search for
     * @param key is the key for tuple creation.
     * @return null, if the id was -1 (no such id), or if nothing was found.
     */
    private Tuple loadAnnotationFinal(long id, String key) throws SQLException {
        Tuple result = null;
        if (id != -1) {
            // order by likelyhood
            result = loadAnnotationString(id, key);
            if (result == null) result = loadAnnotationInteger(id, key);
            if (result == null) result = loadAnnotationFloat(id, key);
            if (result == null) result = loadAnnotationDate(id, key);
            if (result == null) result = loadAnnotationBoolean(id, key);
        }
        return result;
    }

    /**
     * Obtains the value to a specific key in an annotated transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public Tuple loadAnnotationTransformation(String fqdi, String key)
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_tr id
        long id = getAnnotationIdTransformation(did.longValue(), key);

        // no such key, if the id is -1, handled by finalizer
        return loadAnnotationFinal(id, key);
    }

    /**
     * Obtains the value to a specific key in an annotated derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public Tuple loadAnnotationDerivation(String fqdi, String key)
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.DERIVATION);
        if (did == null) throw new SQLException("Unknown DV " + fqdi);

        // obtain possible existing anno_dv id
        long id = getAnnotationIdDerivation(did.longValue(), key);

        // no such key, if the id does not exist
        return loadAnnotationFinal(id, key);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_call id
        long id = getAnnotationIdDeclare(did.longValue(), farg, key);

        // no such key, if the id does not exist
        return loadAnnotationFinal(id, key);
    }

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
            throws SQLException, IllegalArgumentException {
        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain possible existing anno_call id
        long id = getAnnotationIdCall(did.longValue(), index, key);

        // no such key, if the id does not exist
        return loadAnnotationFinal(id, key);
    }

    /**
     * Obtains the value to a specific key in an annotated filename.
     *
     * @param filename is the name of the file that was annotated.
     * @param key is the key to search for
     * @return the annotated value, or null if not found.
     * @see org.griphyn.vdl.classes.LFN
     */
    public Tuple loadAnnotationFilename(String filename, String key)
            throws SQLException, IllegalArgumentException {
        // obtain possible existing anno_lfn id
        long id = getAnnotationIdFilename(filename, key);

        // no such key, if the id does not exist
        return loadAnnotationFinal(id, key);
    }

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
            throws SQLException, IllegalArgumentException {
        Tuple result = null;
        switch (kind) {
            case CLASS_TRANSFORMATION:
                result = loadAnnotationTransformation(primary, key);
                break;
            case CLASS_DERIVATION:
                result = loadAnnotationDerivation(primary, key);
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                result = loadAnnotationCall(primary, ((Integer) secondary).intValue(), key);
                break;
            case CLASS_DECLARE:
                // may throw ClassCastException
                result = loadAnnotationDeclare(primary, ((String) secondary), key);
                break;
            case CLASS_FILENAME:
                result = loadAnnotationFilename(primary, key);
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        return result;
    }

    /**
     * Lists all annotations for a transformation.
     *
     * @param fqdi is the FQDI of the transformation
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Transformation
     */
    public java.util.List loadAnnotationTransformation(String fqdi)
            throws SQLException, IllegalArgumentException {
        java.util.List result = new java.util.ArrayList();

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START select anno_tr2");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_tr2");
        if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
        else ps.setLong(i++, did.longValue());

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_tr");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Tuple temp = loadAnnotationFinal(rs.getLong(1), rs.getString(2));
            if (temp != null) result.add(temp);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_tr2");
        return result;
    }

    /**
     * Lists all annotations for a derivation.
     *
     * @param fqdi is the FQDI of the derivation
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Derivation
     */
    public java.util.List loadAnnotationDerivation(String fqdi)
            throws SQLException, IllegalArgumentException {
        java.util.List result = new java.util.ArrayList();

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.DERIVATION);
        if (did == null) throw new SQLException("Unknown DV " + fqdi);

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START select anno_dv2");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_dv2");
        if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
        else ps.setLong(i++, did.longValue());

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_dv");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Tuple temp = loadAnnotationFinal(rs.getLong(1), rs.getString(2));
            if (temp != null) result.add(temp);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_dv2");
        return result;
    }

    /**
     * Lists all annotations for a formal argument.
     *
     * @param fqdi is the FQDI of the transformation
     * @param farg is the name of the formal argument
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Declare
     */
    public java.util.List loadAnnotationDeclare(String fqdi, String farg)
            throws SQLException, IllegalArgumentException {
        java.util.List result = new java.util.ArrayList();

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START select anno_targ2");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_targ2");
        if (m_dbdriver.preferString()) ps.setString(i++, did.toString());
        else ps.setLong(i++, did.longValue());
        ps.setString(i++, farg);

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_targ");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Tuple temp = loadAnnotationFinal(rs.getLong(1), rs.getString(2));
            if (temp != null) result.add(temp);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_targ2");
        return result;
    }

    /**
     * Lists all annotations for a call statement.
     *
     * @param fqdi is the FQDI of the transformation
     * @param index is the number of the call to annotate.
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.Call
     */
    public java.util.List loadAnnotationCall(String fqdi, int index)
            throws SQLException, IllegalArgumentException {
        java.util.List result = new java.util.ArrayList();

        // split FQDI
        String[] names = Separator.split(fqdi); // may throw IAE

        // obtain DID for FQDI
        Long did = getSpecificDefinitionId(names[0], names[1], names[2], Definition.TRANSFORMATION);
        if (did == null) throw new SQLException("Unknown TR " + fqdi);

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START select anno_call2");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_call2");
        if (m_dbdriver.preferString()) {
            ps.setString(i++, did.toString());
            ps.setString(i++, Integer.toString(index));
        } else {
            ps.setLong(i++, did.longValue());
            ps.setInt(i++, index);
        }

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_call");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Tuple temp = loadAnnotationFinal(rs.getLong(1), rs.getString(2));
            if (temp != null) result.add(temp);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_call2");
        return result;
    }

    /**
     * Lists all annotations for a logical filename.
     *
     * @param filename is the logical filename.
     * @return a list of tuples, which may be empty.
     * @see org.griphyn.vdl.classes.LFN
     */
    public java.util.List loadAnnotationFilename(String filename)
            throws SQLException, IllegalArgumentException {
        java.util.List result = new java.util.ArrayList();

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START select anno_lfn2");
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.anno_lfn2");
        ps.setString(i++, filename);

        Logging.instance().log("chunk", 2, "SELECT id FROM anno_lfn");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Tuple temp = loadAnnotationFinal(rs.getLong(1), rs.getString(2));
            if (temp != null) result.add(temp);
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL select anno_lfn2");
        return result;
    }

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
            throws SQLException, IllegalArgumentException {

        switch (kind) {
            case CLASS_TRANSFORMATION:
                return loadAnnotationTransformation(primary);
            case CLASS_DERIVATION:
                return loadAnnotationDerivation(primary);
            case CLASS_CALL:
                // may throw ClassCastException
                return loadAnnotationCall(primary, ((Integer) secondary).intValue());
            case CLASS_DECLARE:
                // may throw ClassCastException
                return loadAnnotationDeclare(primary, ((String) secondary));
            case CLASS_FILENAME:
                return loadAnnotationFilename(primary);
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }
    }

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
    public java.util.List searchAnnotation(int kind, Object arg, QueryTree tree)
            throws SQLException {
        java.util.List result = new java.util.ArrayList();

        if (tree == null) return result;

        String sql = tree.toSQL(kind, arg);
        if (sql == null || sql.equals("")) return result;

        // obtain all anno_tr ids
        Logging.instance().log("xaction", 1, "START search annotation");

        // use backdoor, why don't we change this into some nicer name???
        ResultSet rs = backdoor(sql);
        while (rs.next()) {
            if (kind == Annotation.CLASS_FILENAME) {
                String fn = rs.getString(1);
                result.add(fn);
            } else {
                Definition d = loadDefinition(rs.getLong(1));
                if (d != null) result.add(d);
            }
        }
        rs.close();

        Logging.instance().log("xaction", 1, "FINAL search annotation");
        return result;
    }

    /**
     * A not very generic method to search annotation (and anything) in the database. Selects any
     * rows in one or more colums from one or more tables restricted by some condition, possibly
     * ordered.
     *
     * <p>WARNING: This is a method for internal use only.
     *
     * @param select is the ordered set of column names to select, or simply a one-value list with
     *     an asterisk.
     * @param table is the name of the table to select from.
     * @param where is a collection of column names and values they must equal.
     * @param order is an optional ordering string.
     * @return something to search for results in.
     * @exception SQLException if something goes wrong with the database.
     * @see org.griphyn.vdl.dbdriver.DatabaseDriver#select( java.util.List, String, java.util.Map,
     *     String )
     */
    public ResultSet searchAnnotation(
            java.util.List select, String table, java.util.Map where, String order)
            throws SQLException {
        return m_dbdriver.select(select, table, where, order);
    }

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value, or have special characters '%'
     * and '_'.
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     * @param type definition type (TR or DV)
     * @return a list of definitions that were deleted.
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List deleteDefinitionEx(
            String namespace, String name, String version, int type) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START delete definitions");

        java.util.List idlist = getDefinitionIdEx(namespace, name, version, type);
        if (idlist.size() == 0) return result;
        // postcondition: contains all IDs, count(id)>0, to be deleted

        // save old values
        if (!m_deferDeleteCommit) {
            // we come from saveDefinition, thus we won't need saved values
            for (Iterator i = idlist.iterator(); i.hasNext(); ) {
                Definition d = loadDefinition(((Long) i.next()).longValue());
                if (d != null) result.add(d);
            }
        }

        // remove all affected annoations by walking through them
        // yuk, this is probably extremely expensive
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            long id = ((Long) i.next()).longValue();
            deleteAnnotationFromDefinition(id);
        }

        // list of all statements we need to access
        PreparedStatement ps[] = {
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_i"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_o"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.lfn_b"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.xml")
        };

        // prepare and batch all statements
        boolean preferString = m_dbdriver.preferString();
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            long id = ((Long) i.next()).longValue();
            for (int j = 0; j < ps.length; ++j) {
                if (preferString) ps[j].setString(1, Long.toString(id));
                else ps[j].setLong(1, id);
                ps[j].addBatch();
            }
        }

        // run all batches
        Logging.instance().log("xaction", 1, "INTER delete definitions");
        for (int j = 0; j < ps.length; ++j) {
            int[] status = new int[idlist.size()];
            try {
                status = ps[j].executeBatch();
            } catch (NullPointerException npe) {
                Logging.instance().log("app", 1, "tripped over NPE, ignoring!");
            }
        }

        Logging.instance().log("xaction", 1, "FINAL delete definitions");
        if (!m_deferDeleteCommit) m_dbdriver.commit();
        return result;
    }

    /**
     * Search the database for definitions by ns::name:version triple and by type (either
     * Transformation or Derivation). This version of the search allows for jokers expressed as null
     * value
     *
     * @param namespace namespace, null to match any namespace
     * @param name name, null to match any name
     * @param version version, null to match any version
     * @param type type of definition, see below, or -1 as wildcard
     * @return a list of Definition items, which may be empty
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     * @see #loadDefinition( String, String, String, int )
     */
    public java.util.List searchDefinitionEx(
            String namespace, String name, String version, int type) throws SQLException {
        java.util.List idlist = getDefinitionIdEx(namespace, name, version, type);

        // TODO: make this a batch or sproc
        java.util.List result = new ArrayList();
        for (Iterator i = idlist.iterator(); i.hasNext(); ) {
            Definition d = loadDefinition(((Long) i.next()).longValue());
            if (d != null) result.add(d);
        }
        return result;
    }

    /**
     * Searches the database for all LFNs that match a certain pattern. The linkage is an additional
     * constraint. This method allows joker characters such as '%' and '_'.
     *
     * @param lfn the LFN name
     * @param link the linkage type of the LFN
     * @return a list of filenames that match the criterion.
     * @see org.griphyn.vdl.classes.LFN#NONE
     * @see org.griphyn.vdl.classes.LFN#INPUT
     * @see org.griphyn.vdl.classes.LFN#OUTPUT
     * @see org.griphyn.vdl.classes.LFN#INOUT
     */
    public java.util.List searchLFN(String lfn, int link) throws SQLException {
        if (lfn == null) throw new NullPointerException("You must query for a filename");
        Logging.instance().log("xaction", 1, "START select LFNs");
        PreparedStatement ps = null;

        if (link == -1) {
            // wildcard match
            ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_*.name.ex");
            for (int ii = 0; ii < c_lfn_names.length; ++ii) ps.setString(ii + 1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct name FROM lfn_*" + " WHERE name LIKE '" + lfn + "'");
        } else if (LFN.isInRange(link)) {
            // known linkage, one table only

            // ordering MUST MATCH classes.LFN constants!
            switch (link) {
                case LFN.NONE:
                    throw new RuntimeException("The linkage \"none\" is not permitted");
                    // break;
                case LFN.INPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_i.name.ex");
                    break;
                case LFN.OUTPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_o.name.ex");
                    break;
                case LFN.INOUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.lfn_b.name.ex");
                    break;
            }
            ;

            ps.setString(1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct name FROM "
                                    + c_lfn_names[link - 1]
                                    + " WHERE name LIKE '"
                                    + lfn
                                    + "'");
        } else {
            throw new RuntimeException("The linkage " + link + " is not permitted");
        }

        ResultSet rs = ps.executeQuery();

        // TODO: make this a batch or sproc
        java.util.List result = new ArrayList();
        while (rs.next()) {
            result.add(rs.getString("name"));
        }

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select LFNs");
        return result;
    }

    /**
     * Searches the database for a list of namespaces of the definitions Sorted in ascending order.
     *
     * @param type type of definition, see below, or -1 for both
     * @return a list of namespaces
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List getNamespaceList(int type) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START select namespaces from DEFINITION");

        java.util.List select = new ArrayList(1);
        select.add(new String("distinct namespace"));

        java.util.Map where = new TreeMap();

        if (type != -1) where.put("type", Integer.toString(type));

        String order = " ORDER BY namespace";
        Logging.instance().log("xaction", 1, "START select namespaces");
        ResultSet rs = m_dbdriver.select(select, "anno_definition", where, order);

        while (rs.next()) result.add(new String(rs.getString("namespace")));

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select namespaces from DEFINITION");
        return result;
    }

    /**
     * Searches the database for a list of fully-qualified names of the definitions sorted in
     * ascending order.
     *
     * @param type type of definition, see below, or -1 for both
     * @return a list of FQDNs
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List getFQDNList(int type) throws SQLException {
        java.util.List result = new ArrayList();
        Logging.instance().log("xaction", 1, "START select triplets from DEFINITION");

        java.util.List select = new ArrayList(1);
        select.add(new String("namespace, name, version"));

        java.util.Map where = new TreeMap();

        if (type != -1) where.put("type", Integer.toString(type));

        String order = " ORDER BY namespace, name, version";
        Logging.instance().log("xaction", 1, "START select triplets");
        ResultSet rs = m_dbdriver.select(select, "anno_definition", where, order);

        while (rs.next())
            result.add(
                    new String(
                            Separator.combine(
                                    rs.getString("namespace"),
                                    rs.getString("name"),
                                    rs.getString("version"))));

        rs.close();
        Logging.instance().log("xaction", 1, "FINAL select triplets from DEFINITION");
        return result;
    }

    /**
     * A too generic method to search annotation (and anything) in the database. This method is also
     * responsible for breaking any no-database-based backends.
     *
     * <p>WARNING: This is a method for internal use only.
     *
     * @param query is an SQL query statement.
     * @return something to search for results in.
     * @exception SQLException if something goes wrong with the database.
     */
    public ResultSet backdoor(String query) throws SQLException {
        return m_dbdriver.backdoor(query);
    }
}
