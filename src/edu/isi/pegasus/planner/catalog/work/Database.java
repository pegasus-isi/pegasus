/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.work;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.planner.catalog.WorkCatalog;
import java.sql.*;
import java.util.*;

/**
 * This class implements a work catalog on top of a simple table in a JDBC database. This enables a
 * variety of replica catalog implementations in a transactionally safe, concurrent environment. The
 * table must be defined using the statements appropriate for your database - they are part of the
 * setup in $PEGASUS_HOME/sql/create-my-wf.sql for MYSQL database and in
 * $PEGASUS_HOME/sql/create-pg-wf.sql.
 *
 * <p>If you chose to use an unsupported database, please check, if your database either supports
 * sequence number, or if it supports auto increment columns. If your database supports sequences
 * (e.g. PostGreSQL), you can use a setup similar to the following (for Oracle, the autoinc can be
 * implemented via a trigger).
 *
 * <pre>
 * CREATE TABLE wf_work (
 *       id         BIGSERIAL PRIMARY KEY,
 *       basedir    TEXT,
 *       vogroup    VARCHAR(255),
 *       workflow   VARCHAR(255),
 *       run        VARCHAR(255),
 *       creator    VARCHAR(32),
 *       ctime      TIMESTAMP WITH TIME ZONE NOT NULL,
 *       state      INTEGER NOT NULL,
 *       mtime      TIMESTAMP WITH TIME ZONE NOT NULL,
 *
 *        CONSTRAINT sk_wf_work UNIQUE(basedir,vogroup,workflow,run)
 * );
 *
 * CREATE TABLE wf_jobstate (
 *       wfid       BIGINT REFERENCES wf_work(id) ON DELETE CASCADE,
 *       jobid      VARCHAR(64),
 *       state      VARCHAR(24) NOT NULL,
 *       mtime      TIMESTAMP WITH TIME ZONE NOT NULL,
 *       site       VARCHAR(64),
 *
 *        CONSTRAINT pk_wf_jobstate PRIMARY KEY (wfid,jobid)
 * );
 * CREATE INDEX ix_wf_jobstate ON wf_jobstate(jobid);
 *
 * CREATE TABLE wf_siteinfo (
 *       id         BIGSERIAL PRIMARY KEY,
 *       handle     VARCHAR(48) NOT NULL,
 *       mtime      TIMESTAMP WITH TIME ZONE,
 *       -- gauges
 *       other      INTEGER DEFAULT 0,
 *       pending    INTEGER DEFAULT 0,
 *       running    INTEGER DEFAULT 0,
 *       -- counters
 *       success    INTEGER DEFAULT 0,
 *       smtime     TIMESTAMP WITH TIME ZONE,
 *       failure    INTEGER DEFAULT 0,
 *       fmtime     TIMESTAMP WITH TIME ZONE,
 *
 *        CONSTRAINT sk_wf_siteinfo UNIQUE(handle)
 * );
 *
 * </pre>
 *
 * In case of databases that do not support sequences (e.g. MySQL), do not specify the <code>
 * create sequence</code>, and use an auto-increment column for the primary key instead, e.g.:
 *
 * <pre>
 * CREATE TABLE wf_work (
 *       id         BIGINT AUTO_INCREMENT PRIMARY KEY,
 *       basedir    TEXT,
 *       vogroup    VARCHAR(255),
 *       workflow   VARCHAR(255),
 *       run        VARCHAR(255),
 *       creator    VARCHAR(32),
 *       ctime      DATETIME NOT NULL,
 *       state      INTEGER NOT NULL,
 *       mtime      DATETIME NOT NULL,
 *
 *        CONSTRAINT sk_wf_work UNIQUE(basedir(255),vogroup,workflow,run)
 * ) type=InnoDB;
 *
 * CREATE TABLE wf_jobstate (
 *       wfid       BIGINT REFERENCES wf_work(id) ON DELETE CASCADE,
 *        jobid      VARCHAR(64),
 *        state      VARCHAR(24) NOT NULL,
 *        mtime      DATETIME NOT NULL,
 *        site       VARCHAR(64),
 *
 *        CONSTRAINT pk_wf_jobstate PRIMARY KEY (wfid,jobid)
 * ) type=InnoDB;
 * CREATE INDEX ix_wf_jobstate ON wf_jobstate(jobid);
 *
 * CREATE TABLE wf_siteinfo (
 *       id         BIGINT AUTO_INCREMENT PRIMARY KEY,
 *       handle     VARCHAR(48) NOT NULL,
 *       mtime      DATETIME,
 *       -- gauges
 *       other      INTEGER DEFAULT 0,
 *       pending    INTEGER DEFAULT 0,
 *       running    INTEGER DEFAULT 0,
 *       -- counters
 *       success    INTEGER DEFAULT 0,
 *       smtime     DATETIME,
 *       failure    INTEGER DEFAULT 0,
 *       fmtime     DATETIME,
 *
 *        CONSTRAINT sk_wf_siteinfo UNIQUE(handle)
 * ) type=InnoDB;
 * </pre>
 *
 * The site attribute should be specified whenever possible. For the shell planner, it will always
 * be of value "local".
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Database implements WorkCatalog {

    /**
     * This message is sent whenever one of the member function is executed which relies on an
     * established database context.
     */
    private static final String mConnectionError = "The database connection is not established";

    /** Maintains the connection to the database over the lifetime of this instance. */
    protected Connection mConnection = null;

    /** Maintains an essential set of prepared statement, ready to use. */
    protected PreparedStatement mStatements[] = null;

    /** The handle to the logging object. */
    protected LogManager mLogger;

    /** The statement to prepare to slurp attributes. */
    private static final String mCStatements[] = {
        // 0:
        "INSERT INTO wf_work(basedir, vogroup, workflow, run, creator, ctime, state, mtime) "
                + "VALUES( ? , ? , ? , ? , ? , ? , ? , ? )",
        // 1:
        "DELETE FROM wf_work WHERE basedir=?   AND vogroup=? AND workflow=? AND  run=? "
    };

    /** Remembers if obtaining generated keys will work or not. */
    private boolean m_autoinc = false;

    /**
     * Convenience c'tor: Establishes the connection to the work catalog database. The usual
     * suspects for the class name include:
     *
     * <pre>
     * org.postgresql.Driver
     * com.mysql.jdbc.Driver
     * com.microsoft.jdbc.sqlserver.SQLServerDriver
     * SQLite.JDBCDriver
     * sun.jdbc.odbc.JdbcOdbcDriver
     * </pre>
     *
     * @param jdbc is a string containing the full name of the java class that must be dynamically
     *     loaded. This is usually an external jar file which contains the Java database driver.
     * @param url is the database driving URL. This string is database specific, and tell the JDBC
     *     driver, at which host and port the database listens, permits additional arguments, and
     *     selects the database inside the rDBMS to connect to. Please refer to your JDBC driver
     *     documentation for the format and permitted values.
     * @param username is the database user account name to connect with.
     * @param password is the database account password to use.
     * @throws LinkageError if linking the dynamically loaded driver fails. This is a run-time
     *     error, and does not need to be caught.
     * @throws ExceptionInInitializerError if the initialization function of the driver's
     *     instantiation threw an exception itself. This is a run-time error, and does not need to
     *     be caught.
     * @throws ClassNotFoundException if the class in your jdbc parameter cannot be found in your
     *     given CLASSPATH environment. Callers must catch this exception.
     * @throws SQLException if something goes awry with the database. Callers must catch this
     *     exception.
     */
    public Database(String jdbc, String url, String username, String password)
            throws LinkageError, ExceptionInInitializerError, ClassNotFoundException, SQLException {
        this();
        // load database driver jar
        Class.forName(jdbc);
        // may throw LinkageError,
        // may throw ExceptionInInitializerError,
        // may throw ClassNotFoundException

        // establish connection to database generically
        connect(url, username, password);
        // may throws SQLException
    }

    /**
     * Default empty constructor creates an object that is not yet connected to any database. You
     * must use support methods to connect before this instance becomes usable.
     *
     * @see #connect( String, String, String )
     */
    public Database() {
        // make connection defunct
        mConnection = null;
        mStatements = null;
        mLogger = LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Connects to the database. This is effectively an accessor to initialize the internal
     * connection instance variable. <b>Warning! You must call {@link java.lang.Class#forName(
     * String )} yourself to load the database JDBC driver jar!</b>
     *
     * @param url is the database driving URL. This string is database specific, and tell the JDBC
     *     driver, at which host and port the database listens, permits additional arguments, and
     *     selects the database inside the rDBMS to connect to. Please refer to your JDBC driver
     *     documentation for the format and permitted values.
     * @param username is the database user account name to connect with.
     * @param password is the database account password to use.
     * @throws SQLException if something goes awry with the database. Callers must catch this
     *     exception.
     * @see java.sql.DriverManager#getConnection( String, String, String )
     */
    public void connect(String url, String username, String password) throws SQLException {
        // establish connection to database generically
        mConnection = DriverManager.getConnection(url, username, password);

        // may throws SQLException
        m_autoinc = mConnection.getMetaData().supportsGetGeneratedKeys();

        // prepared statements are Singletons -- prepared on demand
        mStatements = new PreparedStatement[mCStatements.length];
        for (int i = 0; i < mCStatements.length; ++i) {
            mStatements[i] = null;
        }
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
     * @throws Error subclasses for runtime errors in the class loader.
     */
    public boolean connect(Properties props) {
        boolean result = false;
        // class loader: Will propagate any runtime errors!!!
        String driver = (String) props.remove("db.driver");
        Properties localProps =
                CommonProperties.matchingSubset((Properties) props.clone(), "db", false);

        String url = (String) localProps.remove("url");
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
            mLogger.log("While connecting to Work Catalog", e, LogManager.DEBUG_MESSAGE_LEVEL);
            return result;
        }

        try {
            mConnection = DriverManager.getConnection(url, localProps);
            m_autoinc = mConnection.getMetaData().supportsGetGeneratedKeys();

            // prepared statements are Singletons -- prepared on demand
            mStatements = new PreparedStatement[mCStatements.length];
            for (int i = 0; i < mCStatements.length; ++i) {
                mStatements[i] = null;
            }

            result = true;
        } catch (SQLException e) {
            mLogger.log("While connecting to Work Catalog", e, LogManager.DEBUG_MESSAGE_LEVEL);
            result = false;
        }

        return result;
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
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mConnection == null);
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
     * Inserts a new mapping into the work catalog.
     *
     * @param basedir the base directory
     * @param vogroup the vo to which the user belongs to.
     * @param label the label in the DAX
     * @param run the run number.
     * @param creator the user who is running.
     * @param cTime the creation time of the DAX
     * @param mTime the modification time.
     * @param state the state of the workflow
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int insert(
            String basedir,
            String vogroup,
            String label,
            String run,
            String creator,
            java.util.Date cTime,
            java.util.Date mTime,
            int state)
            throws WorkCatalogException {
        String query = "[no query]";
        int result = 0;
        boolean autoCommitWasOn = false;

        // sanity checks
        if (mConnection == null) {
            throw new RuntimeException(mConnectionError);
        }

        try {
            // delete-before-insert as one transaction
            if ((autoCommitWasOn = mConnection.getAutoCommit())) {
                mConnection.setAutoCommit(false);
            }
            // state == 1

            // // delete before insert...
            this.delete(basedir, vogroup, label, run);
            // state == 2

            int which = 0;
            query = mCStatements[which];

            // sanity checks
            if (mConnection == null) throw new RuntimeException(mConnectionError);

            PreparedStatement ps = getStatement(which);
            ps.setString(1, basedir);
            ps.setString(2, vogroup);
            ps.setString(3, label);
            ps.setString(4, run);
            ps.setString(5, creator);
            ps.setTimestamp(6, new Timestamp(cTime.getTime()));
            ps.setInt(7, state);
            ps.setTimestamp(8, new Timestamp(mTime.getTime()));

            mLogger.log("Executing query " + ps.toString(), LogManager.DEBUG_MESSAGE_LEVEL);

            result = ps.executeUpdate();

        } catch (SQLException e) {
            throw new WorkCatalogException("Unable to insert into work database using " + query, e);
        } finally {
            // restore original auto-commit state
            try {
                if (autoCommitWasOn) {
                    mConnection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                // ignore
            }
        }

        return result;
    }

    /**
     * Deletes a mapping from the work catalog.
     *
     * @param basedir the base directory
     * @param vogroup the vo to which the user belongs to.
     * @param label the label in the DAX
     * @param run the run number.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int delete(String basedir, String vogroup, String label, String run)
            throws WorkCatalogException {

        int result = 0;
        int which = 1;
        String query = mCStatements[which];

        // sanity checks
        if (mConnection == null) throw new RuntimeException(mConnectionError);

        try {
            PreparedStatement ps = getStatement(which);
            ps.setString(1, basedir);
            ps.setString(2, vogroup);
            ps.setString(3, label);
            ps.setString(4, run);

            result = ps.executeUpdate();
        } catch (SQLException e) {
            throw new WorkCatalogException("Unable to delete from database using " + query, e);
        }

        // done
        return result;
    }
}
