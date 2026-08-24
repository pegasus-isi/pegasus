package integrity

import (
	"os"
	"syscall"
	"time"
)

// statDetail carries the raw POSIX stat(2) fields GenerateFullstatYAML
// needs, in a form shared across the (differently-shaped) Linux and Darwin
// syscall.Stat_t.
type statDetail struct {
	Mode                uint32
	Uid, Gid            uint32
	Ino, Nlink          uint64
	Mtime, Atime, Ctime time.Time
}

func statInfo(fi os.FileInfo) statDetail {
	st := fi.Sys().(*syscall.Stat_t)
	return statDetail{
		Mode:  st.Mode,
		Uid:   st.Uid,
		Gid:   st.Gid,
		Ino:   st.Ino,
		Nlink: uint64(st.Nlink),
		Mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Atime: time.Unix(st.Atim.Sec, st.Atim.Nsec),
		Ctime: time.Unix(st.Ctim.Sec, st.Ctim.Nsec),
	}
}
