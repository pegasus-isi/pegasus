package edu.isi.pegasus.gridftp;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

public abstract class Command {
    protected Log logger = LogFactory.getLog(Command.class);
    protected Map<String, GridFTPConnection> connectionCache;
    protected GSSCredential credential = null;
    
    public Command() {
        connectionCache = new HashMap<String, GridFTPConnection>();
    }
    
    public GridFTPConnection connect(GridFTPURL url) throws ConnectException {
        String hostport = url.getHostPort();
        
        if (connectionCache.containsKey(hostport)) {
            logger.debug("Got cached GridFTPClient for "+hostport);
            return connectionCache.get(hostport);
        }
        
        try {
            GridFTPConnection conn = new GridFTPConnection(
                    url.getHost(), url.getPort(), credential);
            connectionCache.put(hostport, conn);
            return conn;
        } catch (Exception e) {
            ConnectException ce = new ConnectException(
                    String.format("Unable to connect to %s", hostport));
            ce.initCause(e);
            throw ce;
        }
    }
    
    /**
     * @param urls The list of URLs to operate on
     * @return True if the operation failed
     */
    public void execute(List<GridFTPURL> urls) throws ConnectException, GridFTPException {
        try {
            GSSManager manager = ExtendedGSSManager.getInstance();
            credential = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
        } catch (GSSException gsse) {
            throw new GridFTPException("Unable to load user proxy", gsse);
        }
        
        try {
            for (GridFTPURL url : urls) {
                run(url);
            }
        } finally {
            for (GridFTPConnection conn : connectionCache.values()) {
                conn.close();
            }
        }
    }
    
    /**
     * @param url The URL to operate on
     * @return True if the operation failed
     */
    protected abstract void run(GridFTPURL url) throws ConnectException, GridFTPException;
}
