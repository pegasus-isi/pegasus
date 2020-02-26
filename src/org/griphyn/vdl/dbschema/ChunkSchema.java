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

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.router.Cache;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class provides basic functionalities to interact with the backend database, such as
 * insertion, deletion, and search of entities in the VDC.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class ChunkSchema extends DatabaseSchema implements VDC {
    /** Name of the four parameter tables in human readable format. */
    protected static final String[] c_lfn_names = {"VDC_NLFN", "VDC_ILFN", "VDC_OLFN", "VDC_BLFN"};

    /** Communication between saveDefinition and deleteDefinition in update mode. */
    protected boolean m_deferDeleteCommit;

    /** An instance of the VDLx XML parser. */
    private org.griphyn.vdl.parser.VDLxParser m_parser;

    /** A cache for definitions to avoid reloading from the database. */
    protected Cache m_cache;

    /**
     * Instantiates an XML parser for VDLx on demand. Since XML parsing XML parsing and parser
     * instantiation is an expensive business, the reader will only be generated on demand.
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
    public ChunkSchema(String dbDriverName)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException {
        // load the driver from the properties
        super(dbDriverName, PROPERTY_PREFIX);
        Logging.instance().log("dbschema", 3, "done with default schema c'tor");

        this.m_cache = this.m_dbdriver.cachingMakesSense() ? new Cache(600) : null;
        this.m_deferDeleteCommit = false;
        this.m_parser = null;

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.definition",
                "INSERT INTO vdc_definition(id,type,name,namespace,version,xml) "
                        + "VALUES (?,?,?,?,?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.nlfn", "INSERT INTO vdc_nlfn(id,name) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.ilfn", "INSERT INTO vdc_ilfn(id,name) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.olfn", "INSERT INTO vdc_olfn(id,name) VALUES (?,?)");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.save.blfn", "INSERT INTO vdc_blfn(id,name) VALUES (?,?)");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.nlfn", "SELECT distinct id FROM vdc_nlfn WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.ilfn", "SELECT distinct id FROM vdc_ilfn WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.olfn", "SELECT distinct id FROM vdc_olfn WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.blfn", "SELECT distinct id FROM vdc_blfn WHERE name=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.all.lfn",
                "SELECT distinct did FROM vdc_nlfn WHERE name=? UNION "
                        + "SELECT distinct did FROM vdc_ilfn WHERE name=? UNION "
                        + "SELECT distinct did FROM vdc_olfn WHERE name=? UNION "
                        + "SELECT distinct did FROM vdc_blfn WHERE name=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.xml.id", "SELECT xml FROM vdc_definition WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.xml",
                "SELECT id,xml FROM vdc_definition WHERE type=? AND name=? AND namespace=? AND version=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.select.id",
                "SELECT id FROM vdc_definition WHERE type=? AND name=? AND namespace=? AND version=?");

        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.xml", "DELETE FROM vdc_definition WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.nlfn", "DELETE FROM vdc_nlfn WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.ilfn", "DELETE FROM vdc_ilfn WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.olfn", "DELETE FROM vdc_olfn WHERE id=?");
        this.m_dbdriver.insertPreparedStatement(
                "stmt.delete.blfn", "DELETE FROM vdc_blfn WHERE id=?");
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
        Logging.instance().log("chunk", 2, "SELECT xml FROM definition");

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
        long probe = -1;
        try {
            Long temp = getDefinitionId(definition);
            if (temp != null) probe = temp.longValue();
        } catch (SQLException e) {
            String cause = e.getMessage();
            Logging.instance()
                    .log("app", 1, "Ignoring SQL exception" + (cause == null ? "" : ": " + cause));
            m_dbdriver.clearWarnings();
        }

        if (probe != -1) {
            if (overwrite) {
                // in overwrite mode, remove old version
                Logging.instance().log("app", 1, "Deleting old " + definition.shortID());

                // remove old definition from database (delete-before-insert)
                try {
                    this.m_deferDeleteCommit = true;
                    deleteDefinition(definition);
                } catch (SQLException e) {
                    String cause = e.getMessage();
                    Logging.instance()
                            .log(
                                    "app",
                                    1,
                                    "Ignoring SQL exception"
                                            + (cause == null ? "" : ": " + e.getMessage()));
                } finally {
                    this.m_deferDeleteCommit = false;
                }
            } else {
                // not overwriting, tell user
                Logging.instance()
                        .log(
                                "app",
                                0,
                                definition.shortID()
                                        + " already exists (SQL vdc_definition.id="
                                        + probe
                                        + "), ignoring");
                return false;
            }
        }

        // Definition is prestine (now)
        Logging.instance().log("app", 1, "Trying to add " + what(definition));

        long id = -1;
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

        // add ID explicitely from sequence to insertion -- -1 is autoinc
        Logging.instance().log("xaction", 1, "START save definition");
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.save.definition");
        int i = 1;
        longOrNull(ps, i++, id);
        ps.setInt(i++, definition.getType());
        if (definition.getName() == null)
            throw new SQLException("VDS inconsistency: The name of a definition is null");
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

        /* NOT YET
         *
        // add to cache
        if ( m_cache != null ) m_cache.set( new Long(id), definition );
         *
         */

        // batch save LFNs from Derivations
        if (definition instanceof Derivation) {
            Derivation derivation = (Derivation) definition;
            Set alreadyKnown = new HashSet();
            // ordering MUST MATCH classes.LFN constants!
            PreparedStatement stmt[] = {
                m_dbdriver.getPreparedStatement("stmt.save.nlfn"),
                m_dbdriver.getPreparedStatement("stmt.save.ilfn"),
                m_dbdriver.getPreparedStatement("stmt.save.olfn"),
                m_dbdriver.getPreparedStatement("stmt.save.blfn")
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
                                    "BATCH INSERT for " + count[ii] + ' ' + c_lfn_names[ii] + 's');

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

                // which one to chose
                int link = lfn.getLink();
                if (!LFN.isInRange(link)) throw new RuntimeException("unknown LFN linkage type");

                int n = 1;
                if (m_dbdriver.preferString()) stmt[link].setString(n++, Long.toString(id));
                else stmt[link].setLong(n++, id);
                stmt[link].setString(n++, name);
                // only keep filenames and linkage in ancillary tables
                // stringOrNull( stmt[link], n++, lfn.getTemporary() );
                // FIXME: dontTransfer, dontRegister?
                stmt[link].addBatch();

                count[link]++;
            }
        }
    }

    //
    // higher level methods, allowing for wildcarding unless working on
    // a single Definition.
    //

    /**
     * Obtains the primary key id for a given definition. "Fake" definitions are permissable. This
     * is an internal helper function.
     *
     * @param d is a definition specification.
     * @return the id of the definition, or null if not found.
     * @see #getDefinitionId( String, String, String, int )
     */
    protected Long getDefinitionId(Definition d) throws SQLException {
        Logging.instance().log("xaction", 1, "START select ID from DEFINITION");
        Long result = null;

        // ps.resetPreparedStatement( "stmt.select.id" );
        int i = 1;
        PreparedStatement ps = m_dbdriver.getPreparedStatement("stmt.select.id");
        ps.setInt(i++, d.getType());
        ps.setString(i++, makeNotNull(d.getName()));
        ps.setString(i++, makeNotNull(d.getNamespace()));
        ps.setString(i++, makeNotNull(d.getVersion()));
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

        Logging.instance().log("xaction", 1, "START select IDs");
        ResultSet rs = m_dbdriver.select(select, "vdc_definition", where, null);

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
        }
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

            Logging.instance().log("xaction", 1, "START DELETE FROM nlfn");
            ps = m_dbdriver.getPreparedStatement("stmt.delete.nlfn");
            if (preferString) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            result = ps.executeUpdate();
            Logging.instance().log("xaction", 1, "FINAL DELETE FROM nlfn: " + result);

            Logging.instance().log("xaction", 1, "START DELETE FROM ilfn");
            ps = m_dbdriver.getPreparedStatement("stmt.delete.ilfn");
            if (preferString) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            result = ps.executeUpdate();
            Logging.instance().log("xaction", 1, "FINAL DELETE FROM ilfn: " + result);

            Logging.instance().log("xaction", 1, "START DELETE FROM olfn");
            ps = m_dbdriver.getPreparedStatement("stmt.delete.olfn");
            if (preferString) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            result = ps.executeUpdate();
            Logging.instance().log("xaction", 1, "FINAL DELETE FROM olfn: " + result);

            Logging.instance().log("xaction", 1, "START DELETE FROM blfn");
            ps = m_dbdriver.getPreparedStatement("stmt.delete.blfn");
            if (preferString) ps.setString(1, Long.toString(id));
            else ps.setLong(1, id);
            result = ps.executeUpdate();
            Logging.instance().log("xaction", 1, "FINAL DELETE FROM blfn: " + result);

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

        // list of all statements we need to access
        PreparedStatement ps[] = {
            this.m_dbdriver.getPreparedStatement("stmt.delete.nlfn"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.ilfn"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.olfn"),
            this.m_dbdriver.getPreparedStatement("stmt.delete.blfn"),
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

        PreparedStatement ps = null;
        if (link == -1) {
            // wildcard match
            ps = this.m_dbdriver.getPreparedStatement("stmt.select.all.lfn");
            for (int ii = 0; ii < c_lfn_names.length; ++ii) ps.setString(ii + 1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct id FROM all lfn" + " WHERE name='" + lfn + "'");
        } else if (LFN.isInRange(link)) {
            // known linkage, one table only
            switch (link) {
                case LFN.NONE:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.nlfn");
                    break;
                case LFN.INPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.ilfn");
                    break;
                case LFN.OUTPUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.olfn");
                    break;
                case LFN.INOUT:
                    ps = this.m_dbdriver.getPreparedStatement("stmt.select.blfn");
                    break;
            }
            ;

            ps.setString(1, lfn);
            Logging.instance()
                    .log(
                            "chunk",
                            2,
                            "SELECT distinct id FROM "
                                    + c_lfn_names[link]
                                    + " WHERE name='"
                                    + lfn
                                    + "'");
        } else {
            throw new RuntimeException("Unknown linkage value " + link);
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
}
