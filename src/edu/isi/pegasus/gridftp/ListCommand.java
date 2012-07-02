package edu.isi.pegasus.gridftp;

import java.net.ConnectException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements the ls command for remote GridFTP servers
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class ListCommand extends Command {
    private Log logger = LogFactory.getLog(ListCommand.class);
    private boolean longFormat = false;
    private boolean dotfiles = false;
    
    public ListCommand(boolean longFormat, boolean dotfiles) {
        this.longFormat = longFormat;
        this.dotfiles = dotfiles;
    }
    
    protected void run(GridFTPURL url) throws ConnectException, GridFTPException {
        GridFTPConnection conn = connect(url);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Listing "+url.toString());
        }
        
        List<FileInfo> listing;
        if (longFormat) {
            listing = conn.ll(url.getPath());
        } else {
            listing = conn.ls(url.getPath());
        }
        
        Collections.sort(listing);
        
        if (listing.size() > 1) {
            System.out.printf("%s:\n", url);
        }
        
        for (FileInfo file : listing) {
            if (dotfiles || !file.getName().startsWith(".")) {
                System.out.println(file);
            }
        }
    }
    
    public static ListCommand fromArguments(List<String> args) throws IllegalArgumentException {
        boolean longFormat = false;
        boolean dotfiles = false;
        
        for (String arg : args) {
            if ("-l".equals(arg)) {
                longFormat = true;
            } else if ("-a".equals(arg)) {
                dotfiles = true;
            } else {
                throw new IllegalArgumentException("Invalid argument: "+arg);
            }
        }
        
        return new ListCommand(longFormat, dotfiles);
    }
}
