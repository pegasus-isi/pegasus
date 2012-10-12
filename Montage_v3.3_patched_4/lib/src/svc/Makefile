CC =		 gcc -g -I. -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

libsvc.a:	svclib.o structlib.o
		ar rv libsvc.a svclib.o structlib.o

svclib.o:	svclib.c svc.h
structlib.o:	structlib.c svc.h

install:
		cp libsvc.a ../..
		cp svc.h ../../include

clean:
		rm -f libsvc.a *.o *.a
