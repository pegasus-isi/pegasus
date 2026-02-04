/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

/**
 * A FileUtility class to use for functions not supported by native JAVA File class.
 *
 * @author Karan Vahi
 */
public class FileUtils {

    /**
     * Copies a file to the specified directory.
     *
     * @param file the file to be copied.
     * @param toDirectory the directory to which the file should be copied.
     * @return file object to the copied file.
     * @throws IOException in case of errors
     */
    public static File copy(File file, File toDirectory) throws IOException {

        if (file == null || !file.exists() || !file.canRead()) {
            throw new IOException("Unable to find source file to be copied " + file);
        }
        String basename = file.getName();

        File destFile = new File(toDirectory, basename.toString());

        if (!toDirectory.exists()) toDirectory.createNewFile();

        FileChannel fcSrc = null;
        FileChannel fcDst = null;
        try {
            fcSrc = new FileInputStream(file).getChannel();
            fcDst = new FileOutputStream(destFile).getChannel();
            fcDst.transferFrom(fcSrc, 0, fcSrc.size());
        } finally {
            if (fcSrc != null) fcSrc.close();
            if (fcDst != null) fcDst.close();
        }

        return destFile;
    }

    /**
     * Copies a file to the specified directory.
     *
     * @param source the source url from where to download
     * @param dest the destination to download to.
     * @return file object to the copied file.
     * @throws IOException in case of errors
     */
    public static File download(String source, String dest) throws IOException {
        return FileUtils.download(new URL(source), new File(dest));
    }
    /**
     * Copies a file to the specified directory.
     *
     * @param source the source url from where to download
     * @param dest the destination to download to.
     * @return file object to the copied file.
     * @throws IOException in case of errors
     */
    public static File download(URL source, File dest) throws IOException {

        ReadableByteChannel rbc = Channels.newChannel(source.openStream());
        FileOutputStream fos = new FileOutputStream(dest);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

        return dest;
    }

    public static void main(String[] args) throws IOException {
        File dest =
                FileUtils.download(
                        "https://raw.githubusercontent.com/pegasushub/pegasus-site-catalogs/refs/heads/main/conf/access-pegasus-annex.yml",
                        "nextflow.config");
        System.out.println("Downloaded file to " + dest.getAbsolutePath());
    }
}
