/**
 * This file or a portion of this file is licensed under the terms of the Globus Toolkit Public
 * License, found at $PEGASUS_HOME/GTPL or http://www.globus.org/toolkit/download/license.html. This
 * notice must appear in redistributions of this file with or without modification.
 *
 * <p>Redistributions of this Software, with or without modification, must reproduce the GTPL in:
 * (1) the Software, or (2) the Documentation or some other similar material which is provided with
 * the Software (if any).
 *
 * <p>Copyright 1999-2004 University of Chicago and The University of Southern California. All
 * rights reserved.
 */
package edu.isi.pegasus.planner.ranking;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.planner.catalog.Catalog;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Properties;

/**
 * This class is responsible for the fetching the DAX'es on the basis of the request ID's from the
 * Windward Provenance Tracking Catalog. If there are more than one way's to get the DAX's then it
 * should be an interface.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class GetDAX {
    /** Prefix for the property subset to use with this catalog. */
    public static final String c_prefix = "pegasus.catalog.provenance.windward";

    /** The DB Driver properties prefix. */
    public static final String DB_PREFIX = "pegasus.catalog.provenance.windward.db";

    /** The statement to prepare to slurp attributes. */
    private static final String mCStatements[] = {
        // 0:
        "SELECT dax FROM instances_and_daxes WHERE seed_id=?",
    };

    /** Maintains the connection to the database over the lifetime of this instance. */
    private Connection mConnection = null;

    /** Maintains an essential set of prepared statement, ready to use. */
    private PreparedStatement mStatements[] = null;

    /** The properties passed to the client. */
    private Properties mProps;

    /** The instance to the Logging manager. */
    private LogManager mLogger;

    /** The default constructor. */
    public GetDAX() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        // make connection defunct
        mConnection = null;
        mStatements = null;
    }

    /**
     * A convenience method to connect on the basis of PegasusProperties. Eventually this logic
     * should go in the invoking code or factory.
     *
     * @param properties PegasusProperties
     * @return boolean
     */
    public boolean connect(PegasusProperties properties) {
        CommonProperties props = properties.getVDSProperties();
        Properties connect = props.matchingSubset(GetDAX.c_prefix, false);

        // get the default db driver properties in first pegasus.catalog.*.db.driver.*
        Properties db = props.matchingSubset(Catalog.DB_ALL_PREFIX, false);
        // now overload with the work catalog specific db properties.
        // pegasus.catalog.work.db.driver.*
        db.putAll(props.matchingSubset(GetDAX.DB_PREFIX, false));

        // to make sure that no confusion happens.
        // add the db prefix to all the db properties
        for (Enumeration e = db.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            connect.put("db." + key, db.getProperty(key));
        }
        return connect(connect);
    }

    /**
     * Establishes a connection to the database from the properties. You can specify a
     * <tt>driver</tt> property to contain the class name of the JDBC driver for your database. This
     * property will be removed before attempting to connect. You must speficy a <tt>url</tt>
     * property to describe the connection. It will be removed before attempting to connect.
     *
     * @param props is the property table with sufficient settings to establish a link with the
     *     database. The minimum key required key is "url", and possibly "driver". Any other keys
     *     depend on the database driver.
     * @return true if connected, false if failed to connect.
     * @see java.sql.DriverManager#getConnection( String, Properties )
     */
    public boolean connect(Properties props) {
        boolean result = false;
        // class loader: Will propagate any runtime errors!!!
        String driver = (String) props.remove("db.driver");
        Properties localProps =
                CommonProperties.matchingSubset((Properties) props.clone(), "db", false);

        String url = (String) localProps.remove("url");
        if (url == null) {
            // try to construct the jdbc string from the properties
            url = getJDBCURL(driver, localProps);
        }
        if (url == null || url.length() == 0) {
            return result;
        }

        try {
            if (driver != null) {
                // only support mysql and postgres for time being
                if (driver.equalsIgnoreCase("MySQL")) {
                    driver = "com.mysql.jdbc.Driver";
                } else if (driver.equalsIgnoreCase("Postgres")) {
                    driver = "org.postgresql.Driver";
                }

                mLogger.log(
                        "Driver being used to connect to Work Catalog is " + driver,
                        LogManager.DEBUG_MESSAGE_LEVEL);

                Class.forName(driver);
            }
        } catch (Exception e) {
            mLogger.log(
                    "While connecting to Windward Provenance Catalog",
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return result;
        }

        try {
            mLogger.log("Connecting with jdbc url " + url, LogManager.DEBUG_MESSAGE_LEVEL);
            mConnection = DriverManager.getConnection(url, localProps);
            //           m_autoinc = mConnection.getMetaData().supportsGetGeneratedKeys();

            // prepared statements are Singletons -- prepared on demand
            mStatements = new PreparedStatement[mCStatements.length];
            for (int i = 0; i < mCStatements.length; ++i) {
                mStatements[i] = null;
            }

            result = true;
        } catch (SQLException e) {
            mLogger.log("While Windward Provenance Catalog", e, LogManager.DEBUG_MESSAGE_LEVEL);
            result = false;
        }

        return result;
    }

    /**
     * Constructs the jdbc url on the basis fo the driver and db properties.
     *
     * @param driver the driver being used.
     * @param properties the db properites
     * @return the jdbc url, else null if unable to construct
     */
    protected String getJDBCURL(String driver, Properties properties) {
        if (driver == null) {
            return null;
        }

        StringBuffer result = new StringBuffer();
        result.append("jdbc:").append(driver.toLowerCase()).append("://");

        String hostname = (String) properties.remove("hostname");
        if (hostname == null || hostname.length() == 0) {
            return null;
        }
        result.append(hostname);

        String database = (String) properties.remove("database");
        if (database == null || database.length() == 0) {
            return null;
        }
        result.append("/").append(database);

        return result.toString();
    }

    /**
     * Given a request ID it fetches the DAX's from the DB and writes out to the directory passed.
     *
     * @param id the request id.
     * @param dir the directory where the DAX'es need to be placed.
     * @return a Collection of basenames fo the DAX'es placed in the directory.
     */
    public Collection<String> get(String id, String dir) {
        if (isClosed()) {
            throw new RuntimeException("The connection to backend database is closed");
        }

        // if
        if (dir == null) {
            throw new RuntimeException("Unable to write out to null directory");
        }

        Collection result = new LinkedList();

        // get the prepared statement
        int which = 0;
        try {
            // do sanity check on dir
            sanityCheck(new File(dir));

            PreparedStatement ps = getStatement(which);
            ps.setString(1, id);

            mLogger.log("Executing query " + ps.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
            ResultSet rs = ps.executeQuery();
            int index = 0;
            while (rs.next()) {
                index++;
                String xml = rs.getString(1);

                // construct the name of the file on index and
                // request id only.
                StringBuffer name = new StringBuffer();
                name.append(id).append("_").append(index);
                name.append(".dax");

                // pipe the dax to  the directory.
                File dax = new File(dir, name.toString());
                PrintWriter pw = new PrintWriter(new FileWriter(dax));
                pw.println(xml);
                pw.close();

                // add to the result
                result.add(dax.getAbsolutePath());
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to query from Windward Provenance Catalog ", e);
        } catch (IOException ioe) {
            throw new RuntimeException("IOException while trying to write to dir " + dir, ioe);
        }

        return result;
    }

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mConnection == null);
    }

    /** Explicitely free resources before the garbage collection hits. */
    public void close() {

        if (mConnection != null) {
            try {
                if (!mConnection.getAutoCommit()) {
                    mConnection.commit();
                }
            } catch (SQLException e) {
                // ignore
            }
        }

        if (mStatements != null) {
            try {
                for (int i = 0; i < mCStatements.length; ++i) {
                    if (mStatements[i] != null) {
                        mStatements[i].close();
                        mStatements[i] = null;
                    }
                }
            } catch (SQLException e) {
                // ignore
            } finally {
                mStatements = null;
            }
        }

        if (mConnection != null) {
            try {
                mConnection.close();
            } catch (SQLException e) {
                // ignore
            } finally {
                mConnection = null;
            }
        }
    }

    /**
     * Singleton manager for prepared statements. This instruction checks that a prepared statement
     * is ready to use, and will create an instance of the prepared statement, if it was unused
     * previously.
     *
     * @param i is the index which prepared statement to check.
     * @return a handle to the prepared statement.
     * @throws SQLException in case of unable to delete entry.
     */
    protected PreparedStatement getStatement(int i) throws SQLException {
        if (mStatements[i] == null) {
            mStatements[i] = mConnection.prepareStatement(mCStatements[i]);
        } else {
            mStatements[i].clearParameters();
        }

        return mStatements[i];
    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck(File dir) throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory " + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException(
                        "Destination "
                                + dir.getPath()
                                + " already "
                                + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {
                throw new IOException("Unable to create  directory " + dir.getPath());
            }
        }
    }

    /**
     * For Testing purposes only.
     *
     * @param args the arguments passed.
     */
    public static void main(String[] args) {
        GetDAX d = new GetDAX();
        LogManagerFactory.loadSingletonInstance().setLevel(LogManager.DEBUG_MESSAGE_LEVEL);

        System.out.println("Connecting to database " + d.connect(PegasusProperties.getInstance()));

        // d.get( "RPaper-ModelerThenClassifier-d3206cf5-b3ad-4c9d-9f08-5d25653d5ccf", null );
        Collection daxes =
                d.get(
                        "RPaper-ModelerThenClassifier-a93169ee-ed72-4d4b-be99-f6d69ae29e04",
                        "/tmp/wings");
        System.out.println("DAX'es written out are " + daxes);
        d.close();
    }
}
