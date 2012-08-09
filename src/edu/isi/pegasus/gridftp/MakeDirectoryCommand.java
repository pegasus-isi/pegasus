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
    private boolean allowExists;
    
    public MakeDirectoryCommand(boolean makeIntermediate, boolean allowExists) {
        this.makeIntermediate = makeIntermediate;
        this.allowExists = allowExists;
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
        try {
            conn.mkdir(path);
        } catch (FileExistsException fee) {
            if (allowExists) {
                logger.warn(fee.getMessage());
            } else {
                throw fee;
            }
        }
    }
    
    public static MakeDirectoryCommand fromArguments(List<String> args) throws IllegalArgumentException {
        boolean makeIntermediate = false;
        boolean allowExists = false;

        for (String arg : args) {
            if (arg.equals("-p")) {
                makeIntermediate = true;
            } else if (arg.equals("-f")) {
                allowExists = true;
            } else {
                throw new IllegalArgumentException("Invalid argument: "+arg);
            }
        }
        
        return new MakeDirectoryCommand(makeIntermediate, allowExists);
    }
}
