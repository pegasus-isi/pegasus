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
package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.StreamGobbler;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.MissingResourceException;

/**
 * This class provides a bridge for creating and initializing transformation catalog on database .
 *
 * @author Prasanth Thomas
 * @version $Revision$
 */
public class CreateTCDatabase {

    /** The default logger. */
    private LogManager mLogger;
    /** Maintains the connection to the database over the lifetime of this instance. */
    protected Connection mConnection = null;

    /** MySQL statement for checking if DB exists */
    private String CHECK_DB_EXISTS_STMT =
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?";

    /** MySQL statement for checking for dropping database */
    private String DROP_DATABASE_STMT = "DROP DATABASE ";

    /** MySQL statement for checking for creating database */
    private String CREATE_DATABASE_STMT = "CREATE DATABASE ";

    /** Stores the driver string */
    private String mDriver;

    /** Stores the database url */
    private String mUrl;

    /** Stores the user name */
    private String mUsername;

    /** Stores the MySQL password */
    private String mPassword;

    /** Stores the database name */
    private String mDatabaseName;

    /** Stores the database host name */
    private String mDatabaseHost;

    /** Stores the absolute path to the mysql home directory */
    private String mDatabaseAbsolutePath;

    /**
     * Creates a JDBCTC instance . Supports only MySQL connection for the time being
     *
     * @param driver the Database driver
     * @param url the Database url
     * @param username the Database user name
     * @param password the Database user password
     * @throws ClassNotFoundException if it fails to load the driver
     * @throws SQLException
     */
    public CreateTCDatabase(
            String driver, String url, String username, String password, String host)
            throws ClassNotFoundException, SQLException {
        mLogger = LogManagerFactory.loadSingletonInstance();

        this.mUrl = url;
        this.mUsername = username;
        this.mPassword = password;
        this.mDatabaseName = getDatabaseName(mUrl);
        this.mDatabaseHost = host;

        if (driver != null) {
            // only support mysql
            if (driver.equalsIgnoreCase("MySQL")) {
                this.mDriver = "com.mysql.jdbc.Driver";
            } else {
                throw new RuntimeException("Only MySQL supported !");
            }
        }

        mDatabaseAbsolutePath = System.getProperty("mysql.home");
        if (mDatabaseAbsolutePath == null) {
            throw new MissingResourceException(
                    "The mysql.home property was not set!", "java.util.Properties", "mysql.home");
        }
        try {
            Class.forName(mDriver);

        } catch (ClassNotFoundException e) {
            mLogger.log("Failed to load driver  " + driver, e, LogManager.DEBUG_MESSAGE_LEVEL);
            throw e;
        }
        try {
            mConnection =
                    DriverManager.getConnection(
                            this.mUrl.substring(0, url.lastIndexOf("/") + 1),
                            this.mUsername,
                            this.mPassword);
        } catch (SQLException e) {
            mLogger.log(
                    "Failed to get connection  " + url + " with user " + username,
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            throw e;
        }
    }

    /**
     * Returns the database name from the database url string
     *
     * @param dbURL the database url string
     * @return the database name, null if it does not contain a database name
     */
    public String getDatabaseName(String dbURL) {

        String databaseName = null;
        int index = -1;
        if ((index = dbURL.lastIndexOf("/")) != -1) {
            if (index < dbURL.length()) {
                databaseName = dbURL.substring(index + 1, dbURL.length());
                if (databaseName.trim().isEmpty()) return null;
            }
        }
        return databaseName;
    }

    /**
     * Checks if the given database exists
     *
     * @param databaseName the database name
     * @return true if database schema exists, false otherwise
     * @throws SQLException
     */
    public boolean checkIfDatabaseExists(String databaseName) throws SQLException {

        PreparedStatement ps;
        ResultSet rs = null;
        try {
            ps = mConnection.prepareStatement(CHECK_DB_EXISTS_STMT);
            ps.setString(1, databaseName);
            rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            mLogger.log(
                    "Failed to check if  " + databaseName + " exists",
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return false;
        } finally {
            if (rs != null) rs.close();
        }
    }

    /**
     * Deletes the database
     *
     * @param databaseName the database
     * @return true, if database is deleted , false otherwise
     * @throws SQLException
     */
    public boolean deleteDatabase(String databaseName) throws SQLException {
        PreparedStatement ps;
        ResultSet rs = null;
        try {
            ps = mConnection.prepareStatement(DROP_DATABASE_STMT + databaseName);
            ps.setString(1, databaseName);
            rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            mLogger.log("Failed to drop  " + databaseName, e, LogManager.DEBUG_MESSAGE_LEVEL);
            return false;
        } finally {
            if (rs != null) rs.close();
        }
    }

    /**
     * Creates data base
     *
     * @param databaseName the database name
     * @return true, if database is created , false otherwise
     * @throws SQLException
     */
    public boolean createDatabase(String databaseName) throws SQLException {

        PreparedStatement ps;
        try {
            ps = mConnection.prepareStatement(CREATE_DATABASE_STMT + databaseName);
            ps.execute();
        } catch (SQLException sqlException) {
            mLogger.log(
                    "Failed to create " + databaseName,
                    sqlException,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return false;
        }
        return true;
    }

    /**
     * Initialize the database with given .sql file
     *
     * @param databaseName the database name
     * @param fileName the file name
     * @return true, if initialization succeeds, false otherwise.
     */
    public boolean initializeDatabase(String databaseName, String fileName) {
        try {
            Runtime r = Runtime.getRuntime();

            // creating the command
            String command =
                    mDatabaseAbsolutePath
                            + File.separator
                            + "bin"
                            + File.separator
                            + "mysql"
                            + " --host "
                            + mDatabaseHost
                            + " --user="
                            + mUsername
                            + " --password="
                            + mPassword
                            + " "
                            + databaseName;
            mLogger.log("Executing command " + command, LogManager.DEBUG_MESSAGE_LEVEL);
            String sqlCommand = "source " + fileName + ";\n";
            String exitCommand = "exit \n";
            Process p = r.exec(command);

            // Sending soure command and exit command to the output stream
            OutputStream os = p.getOutputStream();
            os.write(sqlCommand.getBytes());
            os.flush();
            os.write(exitCommand.getBytes());
            os.flush();
            // spawn off the gobblers with the already initialized default callback

            StreamGobbler ips =
                    new StreamGobbler(
                            p.getInputStream(),
                            new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL));
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            // get the status
            int status = p.waitFor();

            if (status != 0) {
                mLogger.log(
                        "Command "
                                + command
                                + " exited with status "
                                + status
                                + eps.getStackTrace(),
                        LogManager.WARNING_MESSAGE_LEVEL);
                return false;
            }
        } catch (IOException ioe) {
            mLogger.log("IOException while running command ", ioe, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        } catch (InterruptedException ie) {
            mLogger.log(
                    "InterruptedException while running command ",
                    ie,
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        return true;
    }
}
