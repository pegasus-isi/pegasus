package edu.isi.pegasus.gridftp;

import java.io.File;
import java.net.ConnectException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements the mkdir command for remote GridFTP servers
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class MakeDirectoryCommand extends Command {
    private Log logger = LogFactory.getLog(MakeDirectoryCommand.class);
    private boolean makeIntermediate;
    
    public MakeDirectoryCommand(boolean makeIntermediate) {
        this.makeIntermediate = makeIntermediate;
    }
    
    protected void run(GridFTPURL url) throws ConnectException, GridFTPException {
        GridFTPConnection conn = connect(url);
        mkdir(conn, url.getPath());
    }
    
    private void mkdir(GridFTPConnection conn, String path) throws GridFTPException {
        String parent = new File(path).getParent();
        
        if (makeIntermediate && !conn.exists(parent)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Creating intermediate directory "+parent);
            }
            mkdir(conn, parent);
        }
        
        logger.info(conn.getURLFor(path));
        conn.mkdir(path);
    }
    
    public static MakeDirectoryCommand fromArguments(List<String> args) throws IllegalArgumentException {
        boolean makeIntermediate = false;
        
        for (String arg : args) {
            if (arg.equals("-p")) {
                makeIntermediate = true;
            } else {
                throw new IllegalArgumentException("Invalid argument: "+arg);
            }
        }
        
        return new MakeDirectoryCommand(makeIntermediate);
    }
}
