package edu.isi.pegasus.gridftp;

public class DirectoryNotEmptyException extends GridFTPException {
    private static final long serialVersionUID = 1L;
    
    public DirectoryNotEmptyException(String message) {
        super(message);
    }
    
    public DirectoryNotEmptyException(String message, Throwable cause) {
        super(message, cause);
    }
}
