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
import java.nio.channels.FileChannel;

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
}
