# If your system is BSD based, please define BSD specifically.
#
# If you are running Solaris, 
# you need to define _NO_PROTO when you compile them
# you need to link your object files with -lsocket and -lnsl to make executable.


CFLAGS = -g -D_NO_PROTO

SRC = ned_cif.c ned_cli.c ned_err.c ned_sk.c
OBJ = ned_cif.o ned_cli.o ned_err.o ned_sk.o

all : nedname nedpos name_resolver  byname nearname nearposn iau ex_refcode ref

nedname: libnedcli.a nedname.o ../wcstools/libwcs/libwcs.a
	cc -o nedname nedname.o $(OBJ) ../wcstools/libwcs/libwcs.a -lsocket  -lnsl -lm

nedpos: libnedcli.a nedpos.o ../wcstools/libwcs/libwcs.a
	cc -o nedpos nedpos.o $(OBJ) ../wcstools/libwcs/libwcs.a -lsocket  -lnsl -lm

name_resolver: libnedcli.a name_resolver.o
	cc -o name_resolver name_resolver.o libnedcli.a -lm -lsocket  -lnsl

byname: libnedcli.a byname.o
	cc -o byname byname.o $(OBJ)  -lsocket  -lnsl -lm

nearname: libnedcli.a nearname.o
	cc -o nearname nearname.o libnedcli.a -lm -lsocket  -lnsl

nearposn: libnedcli.a nearposn.o
	cc -o nearposn nearposn.o libnedcli.a -lm -lsocket  -lnsl

iau: libnedcli.a iau.o
	cc -o iau iau.o libnedcli.a -lm -lsocket  -lnsl

ex_refcode: libnedcli.a ex_refcode.o
	cc -o ex_refcode ex_refcode.o libnedcli.a -lm -lsocket  -lnsl

ref: libnedcli.a ref.o
	cc -o ref ref.o libnedcli.a -lm -lsocket  -lnsl

libnedcli.a: $(OBJ)
	ar rcu libnedcli.a $(OBJ)

clean:
	rm -f nedname nedpos byname ex_refcode iau name_resolver nearname nearposn ref *.o *.a
