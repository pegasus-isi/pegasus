package edu.isi.pegasus.gridftp;

public class PermissionDeniedException extends GridFTPException {
    private static final long serialVersionUID = 1L;

    public PermissionDeniedException(String message) {
        super(message);
    }
    
    public PermissionDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
