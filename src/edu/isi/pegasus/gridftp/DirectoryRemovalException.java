package edu.isi.pegasus.gridftp;

public class DirectoryRemovalException extends GridFTPException {
    private static final long serialVersionUID = 1L;
    
    public DirectoryRemovalException(String message) {
        super(message);
    }
    
    public DirectoryRemovalException(String message, Throwable cause) {
        super(message, cause);
    }
}
