/**
 * Copyright 2007-2013 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** @author Rajiv Mayani */
public class FileUtilsTest {

    @TempDir File tempDir;

    // -----------------------------------------------------------------------
    // copy(File, File) — happy-path content checks
    // -----------------------------------------------------------------------

    @Test
    public void testCopy_copiesFileContents() throws IOException {
        File src = new File(tempDir, "source.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("hello world");
        }
        File destDir = new File(tempDir, "dest");
        destDir.mkdir();

        File copied = FileUtils.copy(src, destDir);

        assertThat(copied.exists(), is(true));
        assertThat(copied.getName(), is("source.txt"));
        String content = new String(Files.readAllBytes(copied.toPath()));
        assertThat(content, is("hello world"));
    }

    @Test
    public void testCopy_emptyFileIsCopied() throws IOException {
        File src = new File(tempDir, "empty.txt");
        src.createNewFile();
        File destDir = new File(tempDir, "dest-empty");
        destDir.mkdir();

        File copied = FileUtils.copy(src, destDir);

        assertThat(copied.exists(), is(true));
        assertThat(copied.length(), is(0L));
    }

    @Test
    public void testCopy_largeContentIsPreserved() throws IOException {
        File src = new File(tempDir, "large.txt");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10_000; i++) sb.append("line ").append(i).append("\n");
        String expectedContent = sb.toString();
        try (FileWriter fw = new FileWriter(src)) {
            fw.write(expectedContent);
        }
        File destDir = new File(tempDir, "dest-large");
        destDir.mkdir();

        File copied = FileUtils.copy(src, destDir);

        String content = new String(Files.readAllBytes(copied.toPath()));
        assertThat(content, is(expectedContent));
    }

    // -----------------------------------------------------------------------
    // copy(File, File) — return value
    // -----------------------------------------------------------------------

    @Test
    public void testCopy_preservesFileName() throws IOException {
        File src = new File(tempDir, "mydata.csv");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("1,2,3");
        }
        File destDir = new File(tempDir, "output");
        destDir.mkdir();

        File result = FileUtils.copy(src, destDir);
        assertThat(result.getName(), is("mydata.csv"));
    }

    @Test
    public void testCopy_returnsNonNull() throws IOException {
        File src = new File(tempDir, "notnull.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("data");
        }
        File destDir = new File(tempDir, "dest-notnull");
        destDir.mkdir();

        assertThat(FileUtils.copy(src, destDir), is(notNullValue()));
    }

    @Test
    public void testCopy_returnedFileIsInsideDestinationDirectory() throws IOException {
        File src = new File(tempDir, "placed.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("data");
        }
        File destDir = new File(tempDir, "dest-placed");
        destDir.mkdir();

        File result = FileUtils.copy(src, destDir);

        assertThat(result.getParentFile().getCanonicalPath(), is(destDir.getCanonicalPath()));
    }

    // -----------------------------------------------------------------------
    // copy(File, File) — source is unmodified
    // -----------------------------------------------------------------------

    @Test
    public void testCopy_sourceFileIsUnchangedAfterCopy() throws IOException {
        String originalContent = "original content";
        File src = new File(tempDir, "original.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write(originalContent);
        }
        File destDir = new File(tempDir, "dest-unchanged");
        destDir.mkdir();

        FileUtils.copy(src, destDir);

        String srcContent = new String(Files.readAllBytes(src.toPath()));
        assertThat(srcContent, is(originalContent));
    }

    // -----------------------------------------------------------------------
    // copy(File, File) — overwrite existing destination file
    // -----------------------------------------------------------------------

    @Test
    public void testCopy_overwritesExistingFileInDestination() throws IOException {
        File src = new File(tempDir, "overwrite.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("new content");
        }
        File destDir = new File(tempDir, "dest-overwrite");
        destDir.mkdir();
        File existingDest = new File(destDir, "overwrite.txt");
        try (FileWriter fw = new FileWriter(existingDest)) {
            fw.write("old content");
        }

        File copied = FileUtils.copy(src, destDir);

        String content = new String(Files.readAllBytes(copied.toPath()));
        assertThat(content, is("new content"));
    }

    // -----------------------------------------------------------------------
    // copy(File, File) — error paths
    // -----------------------------------------------------------------------

    @Test
    public void testCopy_nullFileThrows() {
        assertThrows(IOException.class, () -> FileUtils.copy(null, tempDir));
    }

    @Test
    public void testCopy_nonExistentFileThrows() {
        File nonExistent = new File(tempDir, "ghost.txt");
        assertThrows(IOException.class, () -> FileUtils.copy(nonExistent, tempDir));
    }

    @Test
    public void testCopy_nonExistentDestinationThrows() throws IOException {
        // toDirectory does not exist: createNewFile() creates a plain file, not a directory.
        // The subsequent attempt to write destFile inside a non-directory then fails with
        // IOException.
        File src = new File(tempDir, "src-for-missing-dest.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("data");
        }
        File missingDestDir = new File(tempDir, "nonexistent-subdir");
        // Do NOT call mkdir() — leave it absent
        assertThrows(
                IOException.class,
                () -> FileUtils.copy(src, missingDestDir),
                "Copying to a non-existent directory should throw IOException");
    }

    // -----------------------------------------------------------------------
    // download(URL, File) — local file:// URL (no network required)
    // -----------------------------------------------------------------------

    @Test
    public void testDownload_urlToFile_copiesContent() throws IOException {
        String content = "downloaded content";
        File srcFile = new File(tempDir, "download-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(content);
        }

        File destFile = new File(tempDir, "download-dest.txt");
        URL fileUrl = srcFile.toURI().toURL();

        File result = FileUtils.download(fileUrl, destFile);

        assertThat(result.exists(), is(true));
        assertThat(result.getCanonicalPath(), is(destFile.getCanonicalPath()));
        assertThat(new String(Files.readAllBytes(result.toPath())), is(content));
    }

    @Test
    public void testDownload_urlToFile_returnsDestFile() throws IOException {
        File srcFile = new File(tempDir, "url-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write("x");
        }
        File destFile = new File(tempDir, "url-dest.txt");
        URL fileUrl = srcFile.toURI().toURL();

        File result = FileUtils.download(fileUrl, destFile);

        assertThat(result, is(sameInstance(destFile)));
    }

    @Test
    public void testDownload_urlToFile_overwritesExistingDest() throws IOException {
        File srcFile = new File(tempDir, "overwrite-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write("fresh data");
        }
        File destFile = new File(tempDir, "overwrite-dest.txt");
        try (FileWriter fw = new FileWriter(destFile)) {
            fw.write("stale data");
        }
        URL fileUrl = srcFile.toURI().toURL();

        FileUtils.download(fileUrl, destFile);

        assertThat(new String(Files.readAllBytes(destFile.toPath())), is("fresh data"));
    }

    // -----------------------------------------------------------------------
    // download(String, String) — local file:// URL
    // -----------------------------------------------------------------------

    @Test
    public void testDownload_stringSrc_stringDest_copiesContent() throws IOException {
        String content = "string-url download";
        File srcFile = new File(tempDir, "str-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(content);
        }
        File destFile = new File(tempDir, "str-dest.txt");

        File result = FileUtils.download(srcFile.toURI().toString(), destFile.getAbsolutePath());

        assertThat(result.exists(), is(true));
        assertThat(new String(Files.readAllBytes(result.toPath())), is(content));
    }

    // -----------------------------------------------------------------------
    // download(String, String, long) — cache / skip-re-download logic
    // -----------------------------------------------------------------------

    @Test
    public void testDownload_withInterval_skipsDownloadWhenFileIsRecent() throws IOException {
        // Create a destination file that was just modified
        File destFile = new File(tempDir, "cached.txt");
        try (FileWriter fw = new FileWriter(destFile)) {
            fw.write("cached content");
        }
        // lastModified is effectively now; updateInterval = 3600 s → no re-download needed
        long updateInterval = 3600L;

        File result =
                FileUtils.download(
                        "file:///nonexistent-url-that-must-not-be-fetched",
                        destFile.getAbsolutePath(),
                        updateInterval);

        // Should return the existing file without touching the (invalid) URL
        assertThat(result, is(notNullValue()));
        assertThat(result.getCanonicalPath(), is(destFile.getCanonicalPath()));
        assertThat(new String(Files.readAllBytes(result.toPath())), is("cached content"));
    }

    @Test
    public void testDownload_withInterval_downloadsWhenFileIsStale() throws IOException {
        String freshContent = "fresh download";
        File srcFile = new File(tempDir, "interval-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(freshContent);
        }

        File destFile = new File(tempDir, "interval-dest.txt");
        try (FileWriter fw = new FileWriter(destFile)) {
            fw.write("stale content");
        }
        // Force the dest file to appear old (modified 2 hours ago)
        long twoHoursAgo = System.currentTimeMillis() - (2 * 3600 * 1000L);
        destFile.setLastModified(twoHoursAgo);

        // updateInterval = 3600 s (1 hour) → file is 2 hours old → must re-download
        File result =
                FileUtils.download(srcFile.toURI().toString(), destFile.getAbsolutePath(), 3600L);

        assertThat(new String(Files.readAllBytes(result.toPath())), is(freshContent));
    }

    @Test
    public void testDownload_withInterval_downloadsWhenFileDoesNotExist() throws IOException {
        String content = "brand new download";
        File srcFile = new File(tempDir, "new-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(content);
        }
        File destFile = new File(tempDir, "new-dest.txt");
        // destFile does not exist yet

        File result =
                FileUtils.download(srcFile.toURI().toString(), destFile.getAbsolutePath(), 3600L);

        assertThat(result.exists(), is(true));
        assertThat(new String(Files.readAllBytes(result.toPath())), is(content));
    }

    @Test
    public void testDownload_withZeroInterval_alwaysRedownloads() throws IOException {
        String freshContent = "always fresh";
        File srcFile = new File(tempDir, "zero-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(freshContent);
        }
        File destFile = new File(tempDir, "zero-dest.txt");
        try (FileWriter fw = new FileWriter(destFile)) {
            fw.write("old content");
        }
        // updateInterval = 0 → any file age forces a re-download
        // Even a file modified just now has (currentTime - lastModified) > 0 * 1000
        destFile.setLastModified(System.currentTimeMillis() - 5000);

        File result =
                FileUtils.download(srcFile.toURI().toString(), destFile.getAbsolutePath(), 0L);

        assertThat(new String(Files.readAllBytes(result.toPath())), is(freshContent));
    }

    @Test
    public void testCopy_destinationPathAlreadyFileThrows() throws IOException {
        File src = new File(tempDir, "src.txt");
        try (FileWriter fw = new FileWriter(src)) {
            fw.write("data");
        }
        File notADirectory = new File(tempDir, "not-a-directory");
        try (FileWriter fw = new FileWriter(notADirectory)) {
            fw.write("existing file");
        }

        assertThrows(IOException.class, () -> FileUtils.copy(src, notADirectory));
    }

    @Test
    public void testDownload_stringSourceInvalidFileUrlThrows() {
        File destFile = new File(tempDir, "invalid-download.txt");
        assertThrows(
                IOException.class,
                () ->
                        FileUtils.download(
                                "file:///definitely/missing/source.txt",
                                destFile.getAbsolutePath()));
    }

    @Test
    public void testDownload_withNegativeIntervalRedownloads() throws IOException {
        String freshContent = "negative interval refresh";
        File srcFile = new File(tempDir, "neg-src.txt");
        try (FileWriter fw = new FileWriter(srcFile)) {
            fw.write(freshContent);
        }
        File destFile = new File(tempDir, "neg-dest.txt");
        try (FileWriter fw = new FileWriter(destFile)) {
            fw.write("stale");
        }

        File result =
                FileUtils.download(srcFile.toURI().toString(), destFile.getAbsolutePath(), -1L);

        assertThat(new String(Files.readAllBytes(result.toPath())), is(freshContent));
    }
}
