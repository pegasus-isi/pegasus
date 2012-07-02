package edu.isi.pegasus.gridftp;

import java.net.MalformedURLException;

import org.globus.util.GlobusURL;

public class GridFTPURL {
    public GlobusURL url;
    
    public GridFTPURL(String url) throws MalformedURLException {
        // Make sure the protocol is correct
        if (!url.startsWith("gsiftp://")) {
            throw new MalformedURLException("Invalid URL: Unknown protocol: "+url);
        }
        
        try {
            this.url = new GlobusURL(url);
        } catch (MalformedURLException e) {
            throw new MalformedURLException("Invalid URL: "+e.getMessage()+": "+url);
        }
        
        // Make sure the host was specified
        String host = this.url.getHost();
        if (host == null || "".equals(host)) {
            throw new MalformedURLException("Invalid URL: No host: " + url);
        }
    }
    
    public String getHost() {
        return url.getHost();
    }
    
    public int getPort() {
        return url.getPort();
    }
    
    public String getHostPort() {
        return String.format("%s:%d", url.getHost(), url.getPort());
    }
    
    public String getPath() {
        String path = url.getPath();
        if (path == null) {
            return "/";
        }
        return "/" + path;
    }
    
    public String toString() {
        return url.getURL();
    }
}
