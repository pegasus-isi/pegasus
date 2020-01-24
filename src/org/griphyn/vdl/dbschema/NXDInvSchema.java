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

import edu.isi.pegasus.planner.invocation.InvocationRecord;
import java.io.*;
import java.lang.reflect.*;
import java.net.InetAddress;
import java.sql.*;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;
import org.xmldb.api.*;
import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;

/**
 * This class provides basic functionalities to interact with the backend database for invocation
 * records, such as insertion, deletion, and search.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class NXDInvSchema extends DatabaseSchema implements PTC {
    private DocumentBuilderFactory m_factory;

    private DocumentBuilder m_builder;

    protected Collection m_db;

    protected Collection m_ptc;

    protected CollectionManagementService m_dbColService;

    protected CollectionManagementService m_ptcColService;

    protected XPathQueryService m_dbQrySvc;

    protected XPathQueryService m_ptcQrySvc;

    /**
     * Default constructor for the provenance tracking.
     *
     * @param dbDriverName is the database driver name
     */
    public NXDInvSchema(String dbDriverName)
            throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException, SQLException, IOException,
                    ParserConfigurationException {
        // load the driver from the properties
        super(); // call minimalistic c'tor, no driver loading!
        ChimeraProperties props = ChimeraProperties.instance();

        m_dbschemaprops = props.getDatabaseSchemaProperties(PROPERTY_PREFIX);

        // extract those properties specific to the database driver.
        // use default settings.
        String driverPrefix = null;
        String driverName = props.getDatabaseDriverName(driverPrefix);
        Properties driverprops = props.getDatabaseDriverProperties(driverPrefix);
        String url = props.getDatabaseURL(driverPrefix);

        try {
            m_factory = DocumentBuilderFactory.newInstance();
            m_builder = m_factory.newDocumentBuilder();

            Class cl = Class.forName(driverName);
            Database database = (Database) cl.newInstance();
            DatabaseManager.registerDatabase(database);

            // get the collection
            m_db = DatabaseManager.getCollection(url + "/db");
            m_dbColService =
                    (CollectionManagementService)
                            m_db.getService("CollectionManagementService", "1.0");

            m_ptc = m_db.getChildCollection("ptc");

            if (m_ptc == null) {
                // collection does not exist, create
                m_ptc = m_dbColService.createCollection("ptc");
            }
            m_ptc.setProperty(OutputKeys.INDENT, "no");

            m_ptcColService =
                    (CollectionManagementService)
                            m_ptc.getService("CollectionManagementService", "1.0");

            m_dbQrySvc = (XPathQueryService) m_db.getService("XPathQueryService", "1.0");

            m_ptcQrySvc = (XPathQueryService) m_ptc.getService("XPathQueryService", "1.0");

            m_dbQrySvc.setProperty("indent", "no");

            m_ptcQrySvc.setProperty("indent", "no");
        } catch (XMLDBException e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Checks the existence of an invocation record in the database. The information is based on the
     * (start,host,pid) tuple, although with private networks, cases may arise that have this tuple
     * identical, yet are different.
     *
     * @param start is the start time of the grid launcher
     * @param host is the address of the host it ran upon
     * @param pid is the process id of the grid launcher itself.
     * @return the id of the existing record, or -1
     */
    public long getInvocationID(java.util.Date start, InetAddress host, int pid)
            throws SQLException {
        long result = -1;
        Logging.instance().log("xaction", 1, "START select invocation id");

        String xquery = "/invocation[@start='" + start + "']";
        xquery += "[@host='" + host.getHostAddress() + "']";
        xquery += "[@pid=" + pid + "]";

        try {
            Logging.instance().log("nxd", 2, xquery);
            ResourceSet rs = m_dbQrySvc.query(xquery);
            ResourceIterator i = rs.getIterator();
            if (i.hasMoreResources()) {
                result = 1;
            } else {
                result = -1;
            }
        } catch (XMLDBException e) {
            throw new SQLException(e.getMessage());
        }

        Logging.instance().log("xaction", 1, "FINAL select invocation id");
        return result;
    }

    /**
     * Inserts an invocation record into the database.
     *
     * @param ivr is the invocation record to store.
     * @return true, if insertion was successful, false otherwise.
     */
    public boolean saveInvocation(InvocationRecord ivr) throws SQLException {
        try {
            StringWriter sw = new StringWriter();

            ivr.toXML(sw, "", null);
            // create new XMLResource; an id will be assigned to the new resource
            XMLResource document = (XMLResource) m_ptc.createResource(null, "XMLResource");
            document.setContent(sw.toString());
            System.out.println(sw.toString());
            m_ptc.storeResource(document);
            return true;
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }
}
