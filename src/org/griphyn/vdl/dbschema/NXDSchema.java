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
import java.util.ArrayList;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import org.griphyn.vdl.annotation.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.router.Cache;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;
import org.w3c.dom.Element;
import org.xmldb.api.*;
import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;

/**
 * This class provides basic functionalities to interact with the backend database, such as
 * insertion, deletion, and search of entities in the VDC.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class NXDSchema extends DatabaseSchema implements XDC {
    /** An instance of the VDLx XML parser. */
    private org.griphyn.vdl.parser.VDLxParser m_parser;

    private DocumentBuilderFactory m_factory;

    private DocumentBuilder m_builder;

    /** reference to collection '/db/vdc'; */
    protected Collection m_db;

    protected Collection m_vdc;

    protected Collection m_meta;

    protected CollectionManagementService m_dbColService;

    protected CollectionManagementService m_vdcColService;

    protected XPathQueryService m_dbQrySvc;

    protected XPathQueryService m_vdcQrySvc;

    protected XPathQueryService m_metaQrySvc;

    protected XUpdateQueryService m_xupdQrySvc;

    /** A cache for definitions to avoid reloading from the database. */
    protected Cache m_cache;

    /**
     * Instantiates an XML parser for VDLx on demand. Since XML parsing and parser instantiation is
     * an expensive business, the reader will only be generated on demand.
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
                Logging.instance().log("nxd", 0, "ignored " + e);
            }
            this.m_parser = new org.griphyn.vdl.parser.VDLxParser(url);
        }

        // done
        return this.m_parser;
    }

    /** Default constructor for the NXD schema. */
    public NXDSchema(String dbDriver)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException,
                    ParserConfigurationException {
        // load the driver from the properties
        super(); // call minimalistic c'tor, no driver loading!
        ChimeraProperties props = ChimeraProperties.instance();

        m_dbschemaprops = props.getDatabaseSchemaProperties(PROPERTY_PREFIX);

        m_cache = null;
        m_parser = null;

        // extract those properties specific to the database driver.
        // use default settings.
        String driverPrefix = null;
        String driverName = props.getDatabaseDriverName(driverPrefix);
        Properties driverprops = props.getDatabaseDriverProperties(driverPrefix);
        String url = props.getDatabaseURL(driverPrefix);
        String user = driverprops.getProperty("user", "guest");
        String passwd = driverprops.getProperty("password", "guest");

        try {
            m_factory = DocumentBuilderFactory.newInstance();
            m_builder = m_factory.newDocumentBuilder();

            Class cl = Class.forName(driverName);
            Database database = (Database) cl.newInstance();
            DatabaseManager.registerDatabase(database);

            // get the collection
            m_db = DatabaseManager.getCollection(url + "/db", user, passwd);
            m_dbColService =
                    (CollectionManagementService)
                            m_db.getService("CollectionManagementService", "1.0");

            m_vdc = m_db.getChildCollection("vdc");

            if (m_vdc == null) {
                // collection does not exist, create
                m_vdc = m_dbColService.createCollection("vdc");
            }
            m_vdc.setProperty(OutputKeys.INDENT, "no");

            m_meta = m_db.getChildCollection("metadata");

            if (m_meta == null) {
                // collection does not exist, create
                m_meta = m_dbColService.createCollection("metadata");
            }
            m_meta.setProperty(OutputKeys.INDENT, "no");

            m_vdcColService =
                    (CollectionManagementService)
                            m_vdc.getService("CollectionManagementService", "1.0");

            m_dbQrySvc = (XPathQueryService) m_db.getService("XPathQueryService", "1.0");

            m_vdcQrySvc = (XPathQueryService) m_vdc.getService("XPathQueryService", "1.0");

            m_metaQrySvc = (XPathQueryService) m_meta.getService("XPathQueryService", "1.0");

            m_dbQrySvc.setProperty("indent", "no");

            m_vdcQrySvc.setProperty("indent", "no");

            m_metaQrySvc.setProperty("indent", "no");

            XUpdateQueryService m_xupdQrySvc =
                    (XUpdateQueryService) m_meta.getService("XUpdateQueryService", "1.0");

        } catch (XMLDBException e) {
            throw new SQLException(e.getMessage());
        }
    }

    private String getDefinitionId(Definition def) {
        String prefix = (def.getType() == Definition.TRANSFORMATION) ? "TR_" : "DV_";
        String version = def.getVersion();
        String suffix = "";
        if (version != null) suffix = "_" + version;
        return prefix + def.getName() + suffix;
    }

    private String getDefinitionId(String name, String version, int type) {
        String prefix = (type == Definition.TRANSFORMATION) ? "TR_" : "DV_";
        String suffix = "";
        if (version != null) suffix = "_" + version;
        return prefix + name + suffix;
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

        try {
            Collection col;
            if (namespace != null) col = m_vdc.getChildCollection(namespace);
            else col = m_vdc;
            String id = getDefinitionId(name, version, type);
            // Logging.instance().log( "nxd", 0, "Definition id " + id );
            XMLResource res = (XMLResource) col.getResource(id);
            if (res != null) {
                MyCallbackHandler cb = new MyCallbackHandler();

                parserInstance()
                        .parse(
                                new org.xml.sax.InputSource(
                                        new StringReader((String) res.getContent())),
                                cb);
                result = cb.getDefinition();

            } else {
                Logging.instance().log("nxd", 0, "Definition not found");
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        Logging.instance().log("xaction", 1, "FINAL load definition");
        return result;
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
        Logging.instance().log("nxd", 2, "START save definition");

        try {
            String namespace = definition.getNamespace();
            Collection col;
            if (namespace != null) col = m_vdc.getChildCollection(namespace);
            else col = m_vdc;
            String id = getDefinitionId(definition);

            if (col == null) {
                // collection does not exist, create
                col = m_vdcColService.createCollection(namespace);
            } else if (!overwrite) {
                if (col.getResource(id) != null) {
                    Logging.instance()
                            .log("app", 0, definition.shortID() + " already exists, ignoring");
                    return false;
                }
            }

            // create new XMLResource; an id will be assigned to the new resource
            XMLResource document = (XMLResource) col.createResource(id, "XMLResource");
            document.setContent(definition.toXML("", null));
            col.storeResource(document);

            // add to cache
            // if ( m_cache != null ) m_cache.set( new Long(id), definition );

        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        // done
        Logging.instance().log("nxd", 2, "FINAL save definition");
        return true;
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
            String namespace = definition.getNamespace();
            Collection col;
            if (namespace != null) col = m_vdc.getChildCollection(namespace);
            else col = m_vdc;
            String id = getDefinitionId(definition);

            if (col != null) {
                if (col.getResource(id) != null) {
                    result = true;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
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
        boolean result = false;

        Logging.instance().log("xaction", 1, "START delete definition");

        try {
            String namespace = definition.getNamespace();
            Collection col;
            if (namespace != null) col = m_vdc.getChildCollection(namespace);
            else col = m_vdc;
            String id = getDefinitionId(definition);

            if (col != null) {
                XMLResource res = (XMLResource) col.getResource(id);
                if (res != null) {
                    col.removeResource(res);
                    result = true;
                }
            }
        } catch (Exception e) {
            // ignore
        }

        Logging.instance().log("xaction", 1, "FINAL delete definition");
        return result;
    }

    /**
     * Delete Definition objects from the database. This method allows for wildcards in the usual
     * fashion. Use null for strings as wildcards, and -1 for the type wildcard.
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
        Logging.instance().log("xaction", 1, "START delete definitions");

        java.util.List result = searchDefinition(namespace, name, version, type);
        for (int i = 0; i < result.size(); i++) deleteDefinition((Definition) result.get(i));

        Logging.instance().log("xaction", 1, "FINAL delete definitions");
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
        String xquery = ""; // "declare namespace vdl='http://www.griphyn.org/chimera/VDL';"

        String triple = "";
        if (namespace != null) triple += "[@namespace='" + namespace + "']";

        if (name != null) triple += "[@name='" + name + "']";

        if (version != null) triple += "[@version='" + version + "']";

        if (type != -1) {
            if (type == Definition.TRANSFORMATION) xquery = "//transformation" + triple;
            else xquery += "//derivation" + triple;
        } else xquery = "//derivation" + triple + "|//transformation" + triple;

        return searchDefinition(xquery);
    }

    /**
     * Searches the database for all derivations that satisfies a certain query.
     *
     * @param xquery the query statement
     * @return a list of definitions
     */
    public java.util.List searchDefinition(String xquery) throws SQLException {
        if (xquery == null) throw new NullPointerException("You must specify a query!");

        java.util.List result = new ArrayList();

        try {
            Logging.instance().log("xaction", 1, "query: " + xquery);

            ResourceSet rs = m_dbQrySvc.query(xquery);
            ResourceIterator i = rs.getIterator();
            while (i.hasMoreResources()) {
                Resource res = i.nextResource();

                MyCallbackHandler cb = new MyCallbackHandler();

                parserInstance()
                        .parse(
                                new org.xml.sax.InputSource(
                                        new StringReader((String) res.getContent())),
                                cb);
                result.add(cb.getDefinition());
            }

        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }

        return result;
    }

    /**
     * Searches the database for elements that satisfies a certain query.
     *
     * @param xquery the query statement
     * @return a list of string
     */
    public java.util.List searchElements(String xquery) throws SQLException {
        if (xquery == null) throw new NullPointerException("You must specify a query!");

        java.util.List result = new ArrayList();

        try {
            Logging.instance().log("nxd", 1, "query: " + xquery);

            ResourceSet rs = m_dbQrySvc.query(xquery);
            ResourceIterator i = rs.getIterator();
            while (i.hasMoreResources()) {
                Resource res = i.nextResource();
                result.add((String) res.getContent());
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        return result;
    }

    /**
     * Searches the database for annotations that satisfies a certain query.
     *
     * @param xquery the query statement
     * @return true if found, false otherwise
     */
    public XMLResource findAnnotation(String xquery) throws SQLException {
        if (xquery == null) return null;

        try {
            Logging.instance().log("nxd", 1, "query: " + xquery);

            ResourceSet rs = m_metaQrySvc.query(xquery);
            ResourceIterator i = rs.getIterator();
            if (i.hasMoreResources()) {
                XMLResource res = (XMLResource) i.nextResource();
                return res;
            } else return null;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    public java.util.List searchFilename(String lfn, int link) throws SQLException {
        if (lfn == null) throw new NullPointerException("You must query for a filename");

        String linkQuery = "";
        String type = LFN.toString(link);
        if (type != null) linkQuery = "[@link = '" + type + "']";

        String xquery = "//derivation[.//lfn[@file = '" + lfn + "']" + linkQuery + "]";
        java.util.List result = searchDefinition(xquery);

        Logging.instance().log("xaction", 1, "FINAL select LFNs");
        return result;
    }

    /**
     * Delete one or more definitions from the backend database. The key triple parameters may be
     * wildcards. Wildcards are expressed as <code>null</code> value, or have regular expression.
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
        Logging.instance().log("xaction", 1, "START delete definitions ex");

        java.util.List result = searchDefinitionEx(namespace, name, version, type);
        for (int i = 0; i < result.size(); i++) deleteDefinition((Definition) result.get(i));

        Logging.instance().log("xaction", 1, "FINAL delete definitions ex");
        return result;
    }

    /**
     * Searches the database for definitions by ns::name:version triple and by type (either
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
        String xquery = "";

        String triple = "";
        if (namespace != null) triple += "[matches(@namespace, '" + namespace + "')]";

        if (name != null) triple += "[matches(@name, '" + name + "')]";

        if (version != null) triple += "[matches(@version, '" + version + "')]";

        if (type != -1) {
            if (type == Definition.TRANSFORMATION) xquery = "//transformation" + triple;
            else xquery += "//derivation" + triple;
        } else xquery = "//derivation" + triple + "|//transformation" + triple;
        return searchDefinition(xquery);
    }

    /**
     * Searches the database for all LFNs that match a certain pattern. The linkage is an additional
     * constraint. This method allows regular expression
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

        String linkQuery = "";
        String type = LFN.toString(link);
        if (type != null) linkQuery = "[@link = '" + type + "')]";

        String xquery =
                //	"//lfn[matches(@file, '" + lfn + "')]" + linkQuery + "/@file";
                "//lfn" + linkQuery + "/@file[matches(.,  '" + lfn + "')]";
        java.util.List result = searchElements(xquery);

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

        String xquery = "";
        if (type == Definition.TRANSFORMATION)
            xquery = "for $n in distinct-values(//transformation/@namespace) order by $n return $n";
        else if (type == Definition.DERIVATION)
            xquery = "for $n in distinct-values(//derivation/@namespace) order by $n return $n";
        else
            xquery =
                    "for $n in distinct-values(//derivation/@namespace|//transformation/@namespace) order by $n return $n";

        java.util.List result = searchElements(xquery);

        Logging.instance().log("xaction", 1, "FINAL select LFNs");
        return result;
    }

    /**
     * Searches the database for a list of fully-qualified names of the definitions sorted in
     * ascending order.
     *
     * @param type type of definition, see below, or -1 for both.
     * @return a list of FQDNs
     * @see org.griphyn.vdl.classes.Definition#TRANSFORMATION
     * @see org.griphyn.vdl.classes.Definition#DERIVATION
     */
    public java.util.List getFQDNList(int type) throws SQLException {
        String xquery = "";

        if (type == Definition.TRANSFORMATION)
            xquery =
                    "for $d in //transformation order by $d/@namespace empty least, $d/@name, $d/@version return string-join((string-join(($d/@namespace, $d/@name), '::'), $d/@version), ':')";
        else if (type == Definition.DERIVATION)
            xquery =
                    "for $d in //derivation order by $d/@namespace empty least, $d/@name, $d/@version return string-join((string-join(($d/@namespace, $d/@name), '::'), $d/@version), ':')";
        else
            xquery =
                    "for $d in (//transformation|//derivation) order by $d/@namespace empty least, $d/@name, $d/@version return string-join((string-join(($d/@namespace, $d/@name), '::'), $d/@version), ':')";

        java.util.List result = searchElements(xquery);

        Logging.instance().log("xaction", 1, "FINAL select LFNs");
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
        String subject = "";
        String select = null;

        switch (kind) {
            case CLASS_TRANSFORMATION:
                subject = "tr";
                break;
            case CLASS_DERIVATION:
                subject = "dv";
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                subject = "tr";
                select = "call[" + ((Integer) secondary).intValue() + "]";
                break;
            case CLASS_DECLARE:
                subject = "tr";
                // may throw ClassCastException
                // select = "declare[@name='" + (String)secondary + "']";
                select = (String) secondary;
                break;
            case CLASS_FILENAME:
                subject = "lfn";
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        try {
            XMLResource res = null;
            String xquery =
                    "/annotation/metadata[@subject=\"" + subject + "\"][@name=\"" + primary + "\"]";
            if (select == null) {
                if (kind != CLASS_FILENAME) {
                    xquery += "[empty(@select)]";
                }
            } else {
                xquery += "[@select=\"" + select + "\"]";
            }

            xquery += "/attribute[@name=\"" + key + "\"]";

            if ((res = findAnnotation(xquery)) != null) {
                String id = res.getDocumentId();

                // get the document
                XMLResource document = (XMLResource) m_meta.getResource(id);
                m_meta.removeResource(document);
                return true;
            }
            return false;
        } catch (XMLDBException e) {
            throw new SQLException(e.getMessage());
        }
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
        return deleteAnnotation(fqdi, null, CLASS_TRANSFORMATION, key);
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
        return deleteAnnotation(fqdi, null, CLASS_DERIVATION, key);
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
        return deleteAnnotation(fqdi, farg, CLASS_DECLARE, key);
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
        return deleteAnnotation(fqdi, new Integer(index), CLASS_CALL, key);
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
        return deleteAnnotation(filename, null, CLASS_FILENAME, key);
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
        /*
           try {
            String key = annotation.getKey();
            String type = annotation.getTypeString();
            Object value = annotation.getValue();

            Logging.instance().log( "nxd", 2, "INSERT INTO anno_tr" );

            String id = null;
            XMLResource res = null;

            String xupdate =
         "<xu:modifications version=\"1.0\" " +
         "xmlns:xu=\"http://www.xmldb.org/xupdate\">";
            String xquery = "//annotation/metadata[@subject='tr'][@name='" +
          fqdi + "']";
            if ((res = findAnnotation(xquery)) != null) {
         //annotation for tr exists
         id = res.getDocumentId();
         String xquery_attr = xquery + "/attribute[@name='" + key + "']/text()";
         if (findAnnotation(xquery_attr)!=null) {
             //attribute already exists
             Logging.instance().log( "nxd", 2, "Attribute already exists." );

             if (!overwrite)
        	 return -1;
             xupdate += "<xu:update select=\"" + xquery + "\">" +
        	 value + "</xu:update>";
             //xupdate += "<xu:update select=\"" + xquery_attr + "/@type\">" +
             //type + "</xu:update>";
         } else {
             //attribute does not exist
             Logging.instance().log( "nxd", 2, "Attribute does not exist." );

             xupdate += "<xu:append select=\"" + xquery + "\">" +
        	 "<xu:element name=\"attribute\">" +
        	 "<xu:attribute name=\"name\">" + key + "</xu:attribute>" +
        	 "<xu:attribute name=\"type\">" + type + "</xu:attribute>" +
        	 value + "</xu:element>" +
        	 "</xu:append>";
         }
         xupdate += "</xu:modifications>";
         System.out.println(xupdate);
         long l = m_xupdQrySvc.update(xupdate);
            } else {
         //create the annotation
         String anno = "<annotation><metadata subject=\"tr\" name=\"" + fqdi + "\">" +
             "<attribute name=\"" + key + "\" type=\"" + type + "\">" + value + "</attribute>" +
             "</metadata></annotation>";

         // create new XMLResource; an id will be assigned to the new resource
         XMLResource document = (XMLResource)m_meta.createResource(null, "XMLResource");
         document.setContent(anno);
         m_meta.storeResource(document);
            }
            return 0;
           } catch (XMLDBException e) {
        throw new SQLException(e.getMessage());
           }
           */
        return saveAnnotation(fqdi, null, CLASS_TRANSFORMATION, annotation, overwrite);
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
        return saveAnnotation(fqdi, null, CLASS_DERIVATION, annotation, overwrite);
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
        return saveAnnotation(fqdi, formalname, CLASS_DECLARE, annotation, overwrite);
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
        return saveAnnotation(fqdi, new Integer(index), CLASS_CALL, annotation, overwrite);
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
        return saveAnnotation(filename, null, CLASS_FILENAME, annotation, overwrite);
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
        String subject = "";
        String select = null;
        String q_sec = null;
        String defn = "transformation";

        switch (kind) {
            case CLASS_TRANSFORMATION:
                subject = "tr";
                break;
            case CLASS_DERIVATION:
                subject = "dv";
                defn = "derivation";
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                subject = "tr";
                select = "call[" + ((Integer) secondary).intValue() + "]";
                q_sec = select;
                break;
            case CLASS_DECLARE:
                subject = "tr";
                // may throw ClassCastException
                q_sec = "declare[@name='" + (String) secondary + "']";
                select = (String) secondary;
                break;
            case CLASS_FILENAME:
                subject = "lfn";
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        try {

            if (kind != CLASS_FILENAME) {
                String[] names = Separator.split(primary);
                String q_ns, q_name, q_ver;

                if (names[0] == null) q_ns = "[empty(@namespace)]";
                else q_ns = "[@namespace='" + names[0] + "']";

                if (names[1] == null) q_name = "[empty(@name)]";
                else q_name = "[@name='" + names[1] + "']";

                if (names[2] == null) q_ver = "[empty(@version)]";
                else q_ver = "[@version='" + names[2] + "']";

                // check if tr/dv is valid
                String xquery = "//" + defn + q_ns + q_name + q_ver;
                if (q_sec != null) xquery += "/" + q_sec;
                Logging.instance().log("nxd", 0, "query: " + xquery);
                ResourceSet rs = m_vdcQrySvc.query(xquery);
                ResourceIterator i = rs.getIterator();
                if (!i.hasMoreResources()) {
                    Logging.instance().log("app", 0, "definition not found!");
                    return -1;
                }
            }

            String key = annotation.getKey();
            String type = annotation.getTypeString();
            Object value = annotation.getValue();

            String id = null;
            XMLResource res = null;
            String xquery =
                    "/annotation/metadata[@subject=\"" + subject + "\"][@name=\"" + primary + "\"]";
            if (select == null) {
                if (kind != CLASS_FILENAME) {
                    xquery += "[empty(@select)]";
                }
            } else {
                xquery += "[@select=\"" + select + "\"]";
            }

            xquery += "/attribute[@name=\"" + key + "\"]";

            if ((res = findAnnotation(xquery)) != null) {
                if (!overwrite) {
                    System.err.println("key " + key + " already defined!");
                    return -1;
                }
                id = res.getDocumentId();
            }

            // create the annotation
            String anno =
                    "<annotation xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><metadata subject=\""
                            + subject
                            + "\" name=\""
                            + primary
                            + "\"";
            if (select != null) anno += " select=\"" + select + "\"";
            anno +=
                    ">"
                            + "<attribute name=\""
                            + key
                            + "\" xsi:type=\"xs:"
                            + type
                            + "\">"
                            + value
                            + "</attribute>"
                            + "</metadata></annotation>";

            // create new XMLResource; an id will be assigned to the new resource
            XMLResource document = (XMLResource) m_meta.createResource(id, "XMLResource");

            document.setContent(anno);
            m_meta.storeResource(document);

            return 0;
        } catch (XMLDBException e) {
            throw new SQLException(e.getMessage());
        }
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
        return loadAnnotation(fqdi, null, CLASS_TRANSFORMATION, key);
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
        return loadAnnotation(fqdi, null, CLASS_DERIVATION, key);
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
        return loadAnnotation(fqdi, farg, CLASS_DECLARE, key);
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
        return loadAnnotation(fqdi, new Integer(index), CLASS_CALL, key);
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
        return loadAnnotation(filename, null, CLASS_FILENAME, key);
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
        String subject = "";
        String select = null;

        switch (kind) {
            case CLASS_TRANSFORMATION:
                subject = "tr";
                break;
            case CLASS_DERIVATION:
                subject = "dv";
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                subject = "tr";
                select = "call[" + ((Integer) secondary).intValue() + "]";
                break;
            case CLASS_DECLARE:
                subject = "tr";
                // may throw ClassCastException
                // select = "declare[@name='" + (String)secondary + "']";
                select = (String) secondary;
                break;
            case CLASS_FILENAME:
                subject = "lfn";
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        try {
            String id = null;
            String xquery =
                    "/annotation/metadata[@subject=\"" + subject + "\"][@name=\"" + primary + "\"]";
            if (select == null) {
                if (kind != CLASS_FILENAME) {
                    xquery += "[empty(@select)]";
                }
            } else {
                xquery += "[@select=\"" + select + "\"]";
            }

            xquery += "/attribute[@name=\"" + key + "\"]";

            XMLResource res = null;
            if ((res = findAnnotation(xquery)) != null) {
                result = loadAnnotationResource(res);
            }
            return result;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /** get the annotation from a XML resource */
    protected Tuple loadAnnotationResource(XMLResource res) throws SQLException {
        Tuple result = null;

        if (res == null) return result;

        Element elem;
        try {
            elem = (Element) res.getContentAsDOM();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (elem != null) {
            String key = elem.getAttribute("name");
            String type = elem.getAttributeNS("http://www.w3.org/2001/XMLSchema-instance", "type");
            String value = elem.getFirstChild().getNodeValue();
            if (key == null || type == null || value == null) return result;
            if (type.equals("xs:string")) {
                result = new TupleString(key, null);
                result.setValue(value);
                return result;
            }
            if (type.equals("xs:float")) {
                result = new TupleFloat(key, 0);
                result.setValue(value);
                return result;
            }
            if (type.equals("xs:int")) {
                result = new TupleInteger(key, 0);
                result.setValue(value);
                return result;
            }
            if (type.equals("xs:boolean")) {
                result = new TupleBoolean(key, false);
                result.setValue(value);
                return result;
            }
            if (type.equals("xs:date")) {
                result = new TupleDate(key, null);
                result.setValue(value);
                return result;
            }
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
        return loadAnnotation(fqdi, null, CLASS_TRANSFORMATION);
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
        return loadAnnotation(fqdi, null, CLASS_DERIVATION);
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
        return loadAnnotation(fqdi, farg, CLASS_DECLARE);
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
        return loadAnnotation(fqdi, new Integer(index), CLASS_CALL);
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
        return loadAnnotation(filename, null, CLASS_FILENAME);
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
        java.util.List result = new java.util.ArrayList();
        String subject = "";
        String select = null;

        switch (kind) {
            case CLASS_TRANSFORMATION:
                subject = "tr";
                break;
            case CLASS_DERIVATION:
                subject = "dv";
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                subject = "tr";
                select = "call[" + ((Integer) secondary).intValue() + "]";
                break;
            case CLASS_DECLARE:
                subject = "tr";
                // may throw ClassCastException
                // select = "declare[@name='" + (String)secondary + "']";
                select = (String) secondary;
                break;
            case CLASS_FILENAME:
                subject = "lfn";
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        try {
            String id = null;
            String xquery =
                    "/annotation/metadata[@subject=\"" + subject + "\"][@name=\"" + primary + "\"]";
            if (select == null) {
                if (kind != CLASS_FILENAME) {
                    xquery += "[empty(@select)]";
                }
            } else {
                xquery += "[@select=\"" + select + "\"]";
            }

            xquery += "/attribute";
            Logging.instance().log("nxd", 1, "query: " + xquery);

            ResourceSet rs = m_metaQrySvc.query(xquery);
            ResourceIterator i = rs.getIterator();
            while (i.hasMoreResources()) {
                XMLResource res = (XMLResource) i.nextResource();
                Tuple tuple = loadAnnotationResource(res);
                if (tuple != null) {
                    result.add(tuple);
                }
            }
            return result;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
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

        String subject = "";
        String defn = "transformation";
        String select = null;

        switch (kind) {
            case CLASS_TRANSFORMATION:
                subject = "tr";
                break;
            case CLASS_DERIVATION:
                subject = "dv";
                defn = "derivation";
                break;
            case CLASS_CALL:
                // may throw ClassCastException
                subject = "tr";
                select = "call[" + ((Integer) arg).intValue() + "]";
                break;
            case CLASS_DECLARE:
                subject = "tr";
                // may throw ClassCastException
                // select = "declare[@name='" + (String)arg + "']";
                select = (String) arg;
                break;
            case CLASS_FILENAME:
                subject = "lfn";
                break;
            default:
                throw new IllegalArgumentException(
                        "The class kind=" + kind + " cannot be annotated");
        }

        try {
            String id = null;
            String cond = "[@subject=\"" + subject + "\"]";
            if (select == null) {
                if (kind != CLASS_FILENAME) {
                    cond += "[empty(@select)]";
                }
            } else {
                cond += "[@select=\"" + select + "\"]";
            }
            String xquery =
                    "for $mn in distinct-values(//annotation/metadata"
                            + cond
                            + "/@name) "
                            + "let $m := //annotation/metadata[@name=$mn]"
                            + cond;

            String where = " where ";
            where += tree.toXQuery("$m/attribute");

            if (kind == CLASS_FILENAME) {
                xquery += ", $r := $m";
                xquery += where;
                xquery += " return $mn";

                return searchElements(xquery);
            } else {
                xquery +=
                        ", $n := substring-before($mn, '::'), $na := substring-after($mn, '::'), $iv := if ($na) then $na else $mn, $v := substring-after($iv, ':'), $ib := substring-before($iv, ':'), $i := if ($ib) then $ib else $iv,";
                xquery +=
                        " $t := if ($n) then if ($v) then //"
                                + defn
                                + "[@namespace=$n][@name=$i][@version=$v] else //"
                                + defn
                                + "[@namespace=$n][@name=$i][empty(@version)] else if ($v) then //"
                                + defn
                                + "[empty(@namespace)][@name=$i][@version=$v] else //"
                                + defn
                                + "[empty(@namespace)][@name=$i][empty(@version)]";

                xquery += where;
                if (kind == CLASS_DECLARE)
                    xquery += " return $t[" + "declare[@name='" + select + "']" + "]";
                else xquery += " return $t";
                return searchDefinition(xquery);
            }

        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * pass-thru to driver.
     *
     * @return true, if it is feasible to cache results from the driver false, if requerying the
     *     driver is sufficiently fast (e.g. driver is in main memory, or driver does caching
     *     itself).
     */
    public boolean cachingMakesSense() {
        return true;
    }

    public void close() throws SQLException {
        try {
            // m_vdc.close();
            // m_db.close();
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }
}
