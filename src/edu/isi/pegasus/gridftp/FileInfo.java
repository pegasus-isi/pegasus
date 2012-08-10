package edu.isi.pegasus.gridftp;

import org.globus.ftp.MlsxEntry;

/**
 * Stores information about a file or directory, such as its name, size, type,
 * etc.
 * 
 * TODO This class should be refactored to store the actual info, instead of
 * just storing the long format.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class FileInfo implements Comparable<FileInfo> {
    private String name;
    private String longFormat;
    
    private FileInfo(String name, String longFormat) {
        this.name = name;
        this.longFormat = longFormat;
    }
    
    @Override
    public int compareTo(FileInfo other) {
        return this.name.compareTo(other.name);
    }
    
    public String getName() {
        return name;
    }
    
    public String toString() {
        return longFormat == null ? name : longFormat;
    }
    
    public boolean isDirectory() {
        if (longFormat == null) {
            throw new IllegalStateException("Unknown file type: use long format listing");
        }
        // TODO Don't rely on longFormat[0] for the type
        return longFormat.charAt(0) == 'd';
    }
    
    public static FileInfo fromLongFormat(String longFormat) {
        int i = longFormat.length();
        while (i>0 && longFormat.charAt(i-1) != ' ') i--;
        String name = longFormat.substring(i);
        return new FileInfo(name, longFormat);
    }
    
    public static FileInfo fromShortFormat(String shortFormat) {
        return new FileInfo(shortFormat, null);
    }

    public static FileInfo fromMlsxEntry(MlsxEntry entry) {
        /* XXX This is ugly, we are just assuming that the type fact begins
           with a 'd' if it is a directory. We really should refactor this
           class to store the raw information and generate the long format. */
        String name = entry.getFileName();
        // The type is 'dir' for directories and symlinks to directories, and 
        // 'file' for files and symlinks to files
        String type = entry.get(MlsxEntry.TYPE); 
        return new FileInfo(name, type);
    }
}
