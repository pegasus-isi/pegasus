package edu.isi.pegasus.gridftp;

import java.io.File;
import java.net.ConnectException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements the rm command for remote GridFTP servers
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class RemoveCommand extends Command {
    private Log logger = LogFactory.getLog(RemoveCommand.class);
    private boolean force;
    private boolean recursive;
    
    public RemoveCommand(boolean force, boolean recursive) {
        this.force = force;
        this.recursive = recursive;
    }
    
    protected void run(GridFTPURL url) throws ConnectException, GridFTPException {
        GridFTPConnection conn = connect(url);
        File file = new File(url.getPath());
        
        try {
            remove(conn, file);
        } catch (NoSuchFileException e) {
            if (!force) {
                throw e;
            }
        }
    }
    
    private void remove(GridFTPConnection conn, File file) throws GridFTPException {
        String path = file.getPath();
        logger.info(conn.getURLFor(path));
        
        try {
            conn.rm(path);
        } catch (DirectoryRemovalException dre) {
            if (recursive) {
                removeDir(conn, file);
            } else {
                throw dre;
            }
        }
    }
    
    private void removeDir(GridFTPConnection conn, File file) throws GridFTPException {
        String path = file.getPath();
        
        if (recursive) {
            for (FileInfo info : conn.ll(path)) {
                String name = info.getName();
                File child = new File(file, name);
                
                if (name.equals(".") || name.equals("..")) {
                    continue;  
                }
                
                if (info.isDirectory()) {
                    removeDir(conn, child);
                } else {
                    remove(conn, child);
                }
            }
        }
        
        logger.info(conn.getURLFor(path));
        conn.rmdir(path);
    }
    
    public static RemoveCommand fromArguments(List<String> args) throws IllegalArgumentException {
        boolean force = false;
        boolean recursive = false;
        
        for (String arg : args) {
            if (arg.equals("-f")) {
                force = true;
            } else if (arg.equals("-r")) {
                recursive = true;
            } else {
                throw new IllegalArgumentException("Invalid argument: "+arg);
            }
        }
        
        return new RemoveCommand(force, recursive);
    }
}
