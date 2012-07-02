package edu.isi.pegasus.gridftp;

public class NoSuchFileException extends GridFTPException {
    private static final long serialVersionUID = 1L;

    public NoSuchFileException(String message) {
        super(message);
    }
    
    public NoSuchFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
