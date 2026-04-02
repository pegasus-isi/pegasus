getif.o: getif.c getif.h utils.h error.h
utils.o: utils.c utils.h
useinfo.o: useinfo.c utils.h useinfo.h
statinfo.o: statinfo.c statinfo.h utils.h checksum.h error.h
jobinfo.o: jobinfo.c getif.h utils.h useinfo.h jobinfo.h statinfo.h \
 procinfo.h ptrace.h parse.h error.h
limitinfo.o: limitinfo.c utils.h limitinfo.h error.h
machine.o: machine.c machine.h machine/basic.h
basic.o: machine/basic.c machine/basic.h machine/../utils.h \
 machine/../error.h
appinfo.o: appinfo.c getif.h utils.h useinfo.h machine.h jobinfo.h \
 statinfo.h procinfo.h ptrace.h appinfo.h limitinfo.h error.h checksum.h
parse.o: parse.c parse.h utils.h error.h
mysystem.o: mysystem.c utils.h appinfo.h statinfo.h jobinfo.h procinfo.h \
 ptrace.h limitinfo.h machine.h mysystem.h error.h
mylist.o: mylist.c mylist.h parse.h error.h
invoke.o: invoke.c invoke.h error.h
pegasus-kickstart.o: pegasus-kickstart.c error.h appinfo.h statinfo.h \
 jobinfo.h procinfo.h ptrace.h limitinfo.h machine.h mysystem.h mylist.h \
 invoke.h utils.h version.h
procinfo.o: procinfo.c procinfo.h ptrace.h utils.h syscall.h error.h
sha2.o: sha2.c sha2.h brg_types.h brg_endian.h
checksum.o: checksum.c sha2.h brg_types.h checksum.h
linux.o: machine/linux.c machine/basic.h machine/linux.h \
 machine/../utils.h machine/../error.h
syscall.o: syscall.c syscall.h ptrace.h error.h
