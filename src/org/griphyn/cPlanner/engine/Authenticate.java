/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.griphyn.cPlanner.engine;


import edu.isi.pegasus.common.logging.LogManagerFactory;
import org.griphyn.cPlanner.classes.AuthenticateRequest;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

import org.globus.gram.Gram;
import org.globus.gram.GramException;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;

import java.util.StringTokenizer;

/**
 * It takes in a authenticate request and authenticates against the resource
 * on the basis of the type of the resource against which authentication is
 * required.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Authenticate {


    /**
     * The standard port at which Grid FTP runs.
     */
    public static final int GRID_FTP_STANDARD_PORT = 2811;

    /**
     * The timeout in seconds. All sockets opened timeout after this period.
     */
    public static final int TIMEOUT_VALUE = 120;

    /**
     * The timeout value that is to be used in milliseconds
     */
    private int mTimeout;

    /**
     * The object containing the authenticate request.
     */
    private AuthenticateRequest mAuthRequest;

    /**
     * The handle to the Pool Info Provider.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * The handle to the LogManager object.
     */
    private LogManager mLogger;

    /**
     * The handle to the PegasusProperties object.
     */
    private PegasusProperties mProps;

    /**
     * The credential to be used while authentication to jobmanager.
     */
    private GSSCredential mCredential;


    /**
     * The overloaded constructor.
     *
     * @param properties  the <code>PegasusProperties</code> to be used.
     */
    public Authenticate( PegasusProperties properties, PoolInfoProvider poolHandle ) {
        mPoolHandle = poolHandle;
        mLogger     =  LogManagerFactory.loadSingletonInstance( );
        mProps      = properties;
        mTimeout    = (mProps.getGridFTPTimeout() == null)?
                      this.TIMEOUT_VALUE:
                      Integer.parseInt(mProps.getGridFTPTimeout());
	mTimeout    *= 1000;
    }

    /**
     * Sets the credential that has to be used for authentication.
     *
     * @param credential  the credential to be set.
     */
    public void setCredential(GSSCredential credential){
        mCredential = credential;
    }


    /**
     * Authenticates against a resource referred to in the authenticate request
     * object.
     */
    public boolean authenticate(AuthenticateRequest ar) {
        mAuthRequest = ar;
        char type = ar.getResourceType();
        boolean alive = false;

        //check if the request is invalid
        if (ar.requestInvalid()) {
            throw new RuntimeException("Invalid authentication request " + ar);
        }

        if (type == AuthenticateRequest.GRIDFTP_RESOURCE) {
            //check if the grid ftp server is alive.
            HostPort hp = getHostPort(ar.getResourceContact());
            alive = gridFTPAlive(hp.getHost(),hp.getPort());

        }
        if (type == AuthenticateRequest.JOBMANAGER_RESOURCE) {
            alive = authenticateJobManager(ar.getResourceContact());
        }

        return alive;
    }




    /**
     * It tries to remove a resource from the soft state of the pool. This is
     * possible only if the underlying pool interface implementation is soft
     * state.
     *
     * @param ar   the AuthenticateRequest containing the resource info
     *
     * @return boolean true removal was successful.
     *                 false unable to remove.
     */
    public boolean removeResource(AuthenticateRequest ar){
        char type = ar.getResourceType();

        if(type == AuthenticateRequest.GRIDFTP_RESOURCE){
            return mPoolHandle.removeGridFtp(ar.getPool(),
                                             ar.getResourceContact());
        }
        if(type == AuthenticateRequest.JOBMANAGER_RESOURCE){
            return mPoolHandle.removeJobManager(ar.getPool(),null,ar.getResourceContact());
        }

        return false;
    }


    /**
     * It authenticates against the jobmanager specifyied.
     *
     * @param contact  the jobmanager contact.
     */
    public boolean authenticateJobManager(String contact){
        boolean val = true;
        try{
            mLogger.log( "Authenticating " + contact, LogManager.DEBUG_MESSAGE_LEVEL);

            if(mCredential == null){
                //try authenticating the default credential
                Gram.ping(contact);
            }
            else
                Gram.ping(mCredential,contact);
        }
        catch(GramException gex){
            mLogger.log("Unable authenticate against jobmanager " +
                        contact + " because " + gex.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            val = false;
        }
        catch(GSSException gss){
            String message = (gss.getMajor() == GSSException.CREDENTIALS_EXPIRED)?
                "Your credentials have expired. You need to do a grid-proxy-init.":
                "GssException caught " +gss.getMajorString()
                + gss.getMinorString();
            mLogger.log(message,LogManager.ERROR_MESSAGE_LEVEL);
            val = false;
        }
        catch(Exception e){
            //an unknown exception occured. print a message and return false
            mLogger.log("Unknown Exception occured " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            val = false;
        }
        finally{
            mLogger.log("Authenticating completed for " + contact,LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return val;
    }

    /**
     * It checks with a grid ftp server running at a particular host
     * and port, to see if it is up or not. This is done by opening a
     * socket to the specified host at the specified port. If the socket
     * timesout (which could be due to excessive load on the server or
     * server being hung) false is returned.
     *
     * @param host  the host at which the gridftp server is running .
     * @param port  the port at which server is running on the host.
     *
     * @return true the gridftp server is alive and kicking.
     *         false - the submit host is not connected to the network.
     *               - the server is not running.
     *               - we were able to connect but timeout.
     *               - version is not compatible.
     *
     */
    public boolean gridFTPAlive(String host, int port) {
        Socket s = new Socket();
        String hp = combine(host, port);
        boolean alive = false;

        mLogger.log("Checking status of " + hp, LogManager.DEBUG_MESSAGE_LEVEL);
        InetSocketAddress addrs = new InetSocketAddress(host, port);
        if (addrs.isUnresolved()) {
            //either the host on which Pegasus is running is not connected
            //to the network, or the hostname is invalid. Either way we return
            //false;
            mLogger.log("Unresolved address to " + hp,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            return false;
        }

        try {
            s.connect(addrs,mTimeout);
            //set the timeout for the input streams
            // gotten from this socket
            s.setSoTimeout(mTimeout);
            String response;
            char type = 'c';
            BufferedReader rd = new BufferedReader(new InputStreamReader(
                s.getInputStream()));

            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
                s.getOutputStream()));

            while ( (response = rd.readLine()) != null) {
                /*mLogger.logMessage("Response from server " + hp + " " +
                                   response,
                                   1);*/

                alive = parseGridFTPResponse(response, type);

                if (type == 'c' && alive) {
                    //send the quit command to the server
                    out.write("quit\r\n");
                    //do a half close. We just need to wait for the response
                    //from server now
                    s.shutdownOutput();
                    type = 'q';
                }
                else {
                    //invalid response or the server is stuck.
                    //break out of the infinite waiting.
                    break;
                }

            }
        }
        catch(java.net.SocketTimeoutException se){
            //means we experienced a timeout on read
            mLogger.log("Timeout experienced while reading from ip" +
                        " stream of " + hp, LogManager.ERROR_MESSAGE_LEVEL);
            alive = false;
        }
        catch (InterruptedIOException e) {
            //timeout was reached.
            mLogger.log("Timeout experienced while contacting " +
                        hp, LogManager.ERROR_MESSAGE_LEVEL);
            alive = false;
        }
        catch (ConnectException ce) {
            //probably no process running at the port
            mLogger.log("GridFtp server on " + host + " not running on port " +
                        port + " .Exception " + ce.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            alive = false;
        }
        catch (IOException ie) {
            mLogger.log("Unable to contact " + hp + " due to " + ie.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            alive = false;
        }
        catch(Exception e){
            //an unknown exception occured. print a message and return false
            mLogger.log("Unknown Exception occured " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            alive = false;
        }
        finally{
            try{
                s.close();
            }
            catch(IOException e){
                mLogger.log("Unable to close socket to " + hp + " because" +
                            e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
                alive = false;
            }
        }


        return alive;
    }

    /**
     * The parses the grid ftp server response and returns if the response
     * was valid or not.
     *
     * @param response the response got from the grid ftp server.
     * @param type     c response when first connected to server.
     *                 q response when sent the quit command.
     *
     * @return boolean true if the response was valid
     *                 false invalid response.
     */
    private boolean parseGridFTPResponse(String response, char type) {
        StringTokenizer st = new StringTokenizer(response);
        boolean valid = false;

        switch (type) {
            case 'c':

                //valid response should be of type 220 blah
                while (st.hasMoreTokens()) {
                    if (st.nextToken().equals("220")) {
                        valid = true;
                    }
                    break;
                }
                break;

            case 'q':

                //valid response would be type 221 blah
                while (st.hasMoreTokens()) {
                    if (st.nextToken().equals("221")) {
                        valid = true;
                    }
                    break;
                }
                break;

            default:
                valid = false;

        }

        if(valid == false)
            mLogger.log(response,LogManager.ERROR_MESSAGE_LEVEL);
        return valid;

    }

    /**
     * A small helper method that returns the standard host and port
     * combination to be used for logging purposes.
     *
     * @param host  the host.
     * @param port  the port.
     *
     * @return combined string.
     */
    private String combine(String host, int port) {
        String st = host + ":" + port;
        return st;
    }

    /**
     * Determines the hostname from the urlPrefix string in the pool file.
     *
     * @param urlPrefix  the protocol, hostname and port combination.
     *
     * @return the host name.
     */
    private HostPort getHostPort(String urlPrefix) {
        StringTokenizer st = new StringTokenizer(urlPrefix);
        String hostPort;
        String hostName = new String();
        String token = new String();
        int count = 0;
        int port = this.GRID_FTP_STANDARD_PORT;
        HostPort hp = null;

        while (st.hasMoreTokens()) {
            token = st.nextToken("/");
            count++;
            if (count == 2) {
                hostPort = token.trim();
                StringTokenizer st1 = new StringTokenizer(hostPort,":");
                hostName = st1.nextToken();
                if(st1.hasMoreTokens()){
                    //port is specified
                    try{
                        port = Integer.parseInt(st1.nextToken());
                    }
                    catch(NumberFormatException e){
                        port = this.GRID_FTP_STANDARD_PORT;
                    }
                }
                //System.out.println("Host->" + hostName + " Port->" + port);
                hp = new HostPort(hostName,port);
                //System.out.println(hp);
                return hp;
            }

        }
        return null;

    }


    /**
     * A convenience inner class that stores the host and the port associated
     * with a server.
     */
    class HostPort{

        /**
         * The host at which the server is running.
         */
        private String mHost;

        /**
         * The port at which the server is running.
         */
        private int mPort;

        /**
         * The overloaded constructor
         */
        public HostPort(String host, int port){
            mHost = host;
            mPort = port;
        }

        /**
         * Returns the host associated with this object.
         *
         * @return String
         */
        public String getHost(){
            return mHost;
        }


        /**
         * Returns the port associated with this object.
         *
         * @return int
         */
        public int getPort(){
            return mPort;
        }

        /**
         * Returns the string version of this object.
         */
        public String toString(){
            StringBuffer sb = new StringBuffer();
            sb.append("host name ").append(mHost).
                append(" port ").append(mPort);

            return sb.toString();
        }
    }

    public static void main(String[] args){
        Authenticate a = new Authenticate( PegasusProperties.getInstance(), null );
        String contact = "dc-user2.isi.edu/jobmanager-lsf";
        String contact1 = "dc-n1.isi.edu";
        System.out.println("Authenticating " + contact1);
        //a.authenticateJobManager(contact);
        a.gridFTPAlive("dc-n1.isi.edu",a.GRID_FTP_STANDARD_PORT);
    }
}
