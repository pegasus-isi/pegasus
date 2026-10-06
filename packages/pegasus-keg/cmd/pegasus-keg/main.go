// Command pegasus-keg is the Kanonical Executable for Grids: a synthetic job
// that stands in for real application binaries in Pegasus test workflows.
// It is a Go port of the original C++ pegasus-keg.cc: same flags, same
// output format, same exit codes.
package main

import (
	"os"

	"github.com/pegasus-isi/pegasus/packages/pegasus-keg/internal/keg"
)

func main() {
	os.Exit(keg.Run(os.Args, os.Stdin, os.Stdout, os.Stderr))
}
