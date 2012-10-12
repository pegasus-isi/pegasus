CC = gcc -g -I . -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

libmtbl.a:	mtbl.c
		$(CC) -c mtbl.c
		ar rv libmtbl.a mtbl.o

install:
		cp libmtbl.a ../..
		cp mtbl.h ../../include

clean:		
		rm -f libmtbl.a *.o *.a core
