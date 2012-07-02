package edu.isi.pegasus.gridftp;

public class GridFTPException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public GridFTPException(String message) {
        super(message);
    }
    
    public GridFTPException(String message, Throwable cause) {
        super(message, cause);
    }
}
