package edu.isi.pegasus.gridftp;

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
}
