package edu.isi.pegasus.gridftp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.Buffer;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.DataSink;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.Session;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.ietf.jgss.GSSCredential;

/**
 * A connection to a GridFTP server
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class GridFTPConnection {
    private Log logger = LogFactory.getLog(GridFTPConnection.class);
    private String host;
    private int port;
    private GSSCredential credential;
    private GridFTPClient client;
    
    public GridFTPConnection(String host, int port, GSSCredential credential) throws Exception {
        this.host = host;
        this.port = port;
        this.credential = credential;
        
        client = new GridFTPClient(host, port);
        
        client.setAuthorization(HostAuthorization.getInstance());
        client.authenticate(this.credential);
        
        client.setDataChannelAuthentication(DataChannelAuthentication.NONE);
        
        client.setMode(Session.MODE_STREAM);
        client.setType(Session.TYPE_ASCII);
    }
    
    /**
     * List contents of path in long format
     */
    public List<FileInfo> ll(String path) throws GridFTPException {
        return ls(path, true);
    }
    
    /**
     * List contents of path in short (filename only) format
     */
    public List<FileInfo> ls(String path) throws GridFTPException {
        return ls(path, false);
    }
    
    /**
     * List contents of path. If longFormat is true, return the long format listing.
     */
    private List<FileInfo> ls(String path, boolean longFormat) throws GridFTPException {
        // We need to create a new data channel every time because
        // the API will not cache the data channel connection if the
        // server is in passive mode.
        try {
            client.setPassive();
            client.setLocalActive();
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
        
        StringBuilderDataSink sink = new StringBuilderDataSink();
        
        try {
            if (longFormat) {
                client.list(path, null, sink);
            } else {
                client.nlist(path, sink);
            }
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
        
        BufferedReader reader =
            new BufferedReader(new StringReader(sink.toString()));
        
        List<FileInfo> listing = new ArrayList<FileInfo>();
        
        try {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                FileInfo file;
                if (longFormat) {
                    file = FileInfo.fromLongFormat(line);
                } else {
                    file = FileInfo.fromShortFormat(line);
                }
                
                listing.add(file);
            }
            
            reader.close();
        } catch (IOException ioe) {
            throw new GridFTPException("Error reading directory listing");
        }
        
        return listing;
    }
    
    /**
     * Remove path
     */
    public void rm(String path) throws GridFTPException {
        try {
            client.deleteFile(path);
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
    }
    
    /**
     * Remove directory path
     */
    public void rmdir(String path) throws GridFTPException {
        try {
            client.deleteDir(path);
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
    }
    
    /**
     * Create directory path
     */
    public void mkdir(String path) throws GridFTPException {
        try {
            client.makeDir(path);
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
    }
    
    /**
     * Return true if path exists, false otherwise
     */
    public boolean exists(String path) throws GridFTPException {
        try {
            stat(path);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }
    
    /**
     * Return information about path
     */
    private MlsxEntry stat(String path) throws GridFTPException {
        try {
            return client.mlst(path);
        } catch (Exception e) {
            translateException(e, getURLFor(path));
        }
        throw new IllegalStateException("Should not reach end");
    }
    
    /**
     * Translate the exception e into one of the subclasses of GridFTPException
     */
    private void translateException(Exception e, String message) throws GridFTPException {
        String cause = e.getMessage();
        if (cause.contains("No such file or directory")) {
            throw new NoSuchFileException("No such file or directory: "+message, e);
        } else if (cause.contains("Is a directory")) {
            throw new DirectoryRemovalException("Directory removal: "+message, e);
        } else if (cause.contains("Directory not empty")){
            throw new DirectoryNotEmptyException("Directory not empty: "+message, e);
        } else if (cause.contains("File exists")) {
            throw new FileExistsException("File exists: "+message, e);
        } else if (cause.contains("Permission denied")) {
            throw new PermissionDeniedException("Permission denied: "+message, e);
        } else {
            throw new GridFTPException(message, e);
        }
    }
    
    /**
     * Return a gsiftp:// URL for path
     */
    public String getURLFor(String path) {
        return String.format("gsiftp://%s:%d%s", host, port, path);
    }
    
    /**
     * Close the connection
     */
    public void close() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "Closing connection to %s:%d", host, port));
        }
        try {
            client.close();
        } catch (Exception e) {
            logger.error(String.format(
                    "Error closing connection to %s:%d", host, port), e);
        }
    }
    
    /**
     * This class is used to collect data returned by the list command into
     * a StringBuilder.
     */
    private class StringBuilderDataSink implements DataSink {
        private StringBuilder builder;
        
        public StringBuilderDataSink() {
            builder = new StringBuilder();
        }
        
        public void write(Buffer buffer) throws IOException {
            builder.append(new String(
                    buffer.getBuffer(), 0, buffer.getLength(), "utf-8"));
        }
        
        public void close() throws IOException {
        }
        
        public String toString() {
            return builder.toString();
        }
    }
}
