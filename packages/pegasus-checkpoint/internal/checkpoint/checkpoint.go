// Package checkpoint implements the file-matching, archiving, and stage-out
// logic behind pegasus-checkpoint: periodically (or on SIGUSR1) archive a set
// of pattern-matched files/directories in the job's CWD into a single
// compressed tarball, then stage it back to the workflow's staging site via
// pegasus-transfer.
//
// This is a Go port of the Python packages/pegasus-worker/src/Pegasus/cli/
// pegasus-checkpoint.py script. It is not byte-for-byte compatible with
// Python's tarfile output (uname/gname/exact header fields differ), but the
// checkpoint archive is an opaque payload as far as the rest of Pegasus is
// concerned — nothing downstream parses its tar headers, it is just staged
// out and later extracted by hand — so a standard, valid tar.gz produced by
// Go's stdlib is sufficient; only the archive name and the file set matter.
package checkpoint

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// CheckpointFilename is the name of the archived & compressed checkpoint,
// matching pegasus-checkpoint.py's CHECKPOINT_FILENAME.
const CheckpointFilename = "pegasus.checkpoint.tar.gz"

// TransferURLFile is the pegasus-transfer url file expected to be generated
// by pegasus-lite, matching PEGASUS_TRANSFER_URL_FILE.
const TransferURLFile = "pegasus_checkpoint_transfer_urls.json"

// PIDFile is the file this process's PID is written to on startup, matching
// PEGASUS_CHECKPOINT_PID_FILE.
const PIDFile = "pegasus_checkpoint.pid"

// WritePID writes the current process's PID to PIDFile.
func WritePID() error {
	return os.WriteFile(PIDFile, []byte(strconv.Itoa(os.Getpid())), 0o644)
}

// MatchedFilenames returns the set of entries in dir whose name fullmatches
// one of the given regex patterns, matching
// CheckpointWorker.get_matched_filenames. Returned in sorted order (the
// Python original iterates a set, whose order is unspecified — sorting here
// is a strict improvement, not a compatibility concern, since tar entry
// order doesn't matter to anything that reads the checkpoint archive back).
func MatchedFilenames(dir string, patterns []string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	compiled := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		re, err := regexp.Compile("^(?:" + p + ")$") // emulate re.fullmatch
		if err != nil {
			return nil, fmt.Errorf("invalid pattern %q: %w", p, err)
		}
		compiled[i] = re
	}

	matched := map[string]bool{}
	for _, re := range compiled {
		for _, e := range entries {
			if re.MatchString(e.Name()) {
				matched[e.Name()] = true
			}
		}
	}

	names := make([]string, 0, len(matched))
	for n := range matched {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// ArchiveAndCompress archives and gzip-compresses the given filenames (paths
// relative to the current working directory) into CheckpointFilename,
// matching CheckpointWorker.archive_and_compress. Directories are added
// recursively; symlinks are stored as symlinks, not dereferenced.
func ArchiveAndCompress(filenames []string) error {
	f, err := os.Create(CheckpointFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	for _, name := range filenames {
		if err := addToTar(tw, name); err != nil {
			return fmt.Errorf("archiving %s: %w", name, err)
		}
	}
	return nil
}

// addToTar mirrors tarfile.add(name, recursive=True)'s default behavior:
// symlinks are recorded as symlinks (not followed), directories are recorded
// and then recursed into (children visited in sorted order), regular files
// are recorded with their contents.
func addToTar(tw *tar.Writer, name string) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(name)
		if err != nil {
			return err
		}
		hdr := &tar.Header{
			Typeflag: tar.TypeSymlink,
			Name:     name,
			Linkname: target,
			Mode:     int64(info.Mode().Perm()),
			ModTime:  info.ModTime(),
		}
		return tw.WriteHeader(hdr)
	}

	if info.IsDir() {
		hdr := &tar.Header{
			Typeflag: tar.TypeDir,
			Name:     name + "/",
			Mode:     int64(info.Mode().Perm()),
			ModTime:  info.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		children, err := os.ReadDir(name)
		if err != nil {
			return err
		}
		sort.Slice(children, func(i, j int) bool { return children[i].Name() < children[j].Name() })
		for _, c := range children {
			if err := addToTar(tw, filepath.Join(name, c.Name())); err != nil {
				return err
			}
		}
		return nil
	}

	hdr := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     name,
		Size:     info.Size(),
		Mode:     int64(info.Mode().Perm()),
		ModTime:  info.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(tw, f)
	return err
}
