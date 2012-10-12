# If your system is BSD based, please define BSD specifically.
#
# If you are running Solaris, 
# you need to define _NO_PROTO when you compile them
# you need to link your object files with -lsocket and -lnsl to make executable.


CFLAGS = -g -D_NO_PROTO
CC = cc

SRC = ned_cli.c ned_cif.c ned_err.c ned_sk.c
OBJ = ned_cli.o ned_cif.o ned_err.o ned_sk.o

libned.a:	$(OBJ)
	ar rv $@ $?
	ranlib $@

all : nedname nedpos name_resolver  byname nearname nearposn iau ex_refcode ref

nedname: libned.a nedname.o ../libwcs/libwcs.a
	cc -o nedname nedname.o $(OBJ) ../libwcs/libwcs.a -lsocket  -lnsl -lm

nedpos: libned.a nedpos.o ../libwcs/libwcs.a
	cc -o nedpos nedpos.o $(OBJ) ../libwcs/libwcs.a -lsocket  -lnsl -lm

name_resolver: libned.a name_resolver.o
	cc -o name_resolver name_resolver.o libned.a -lm -lsocket  -lnsl

byname: libned.a byname.o
	cc -o byname byname.o $(OBJ)  -lsocket  -lnsl -lm

nearname: libned.a nearname.o
	cc -o nearname nearname.o libned.a -lm -lsocket  -lnsl

nearposn: libned.a nearposn.o
	cc -o nearposn nearposn.o libned.a -lm -lsocket  -lnsl

iau: libned.a iau.o
	cc -o iau iau.o libned.a -lm -lsocket  -lnsl

ex_refcode: libned.a ex_refcode.o
	cc -o ex_refcode ex_refcode.o libned.a -lm -lsocket  -lnsl

ref: libned.a ref.o
	cc -o ref ref.o libned.a -lm -lsocket  -lnsl

clean:
	rm -f nedname nedpos byname ex_refcode iau name_resolver nearname nearposn ref *.o *.a
