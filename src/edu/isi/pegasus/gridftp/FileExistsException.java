package edu.isi.pegasus.gridftp;

public class FileExistsException extends GridFTPException {
    private static final long serialVersionUID = 1L;

    public FileExistsException(String message) {
        super(message);
    }
    
    public FileExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
