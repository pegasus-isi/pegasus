package integrity

import (
	"os"
	"syscall"
	"time"
)

// statDetail carries the raw POSIX stat(2) fields generate_fullstat_yaml
// needs, in a form shared across the (differently-shaped) Linux and Darwin
// syscall.Stat_t.
type statDetail struct {
	Mode                uint32
	Uid, Gid            uint32
	Ino, Nlink          uint64
	Mtime, Atime, Ctime time.Time
}

func statFile(fi os.FileInfo) statDetail {
	st := fi.Sys().(*syscall.Stat_t)
	return statDetail{
		Mode:  uint32(st.Mode),
		Uid:   st.Uid,
		Gid:   st.Gid,
		Ino:   st.Ino,
		Nlink: uint64(st.Nlink),
		Mtime: time.Unix(st.Mtimespec.Sec, st.Mtimespec.Nsec),
		Atime: time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec),
		Ctime: time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec),
	}
}
