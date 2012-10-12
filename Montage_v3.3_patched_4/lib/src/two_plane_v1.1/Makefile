CC      =       gcc
CFLAGS  =       -c -ansi -Wall -g -I. -I../../include -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

libtwoplane.a:	two_plane.o distort.o initdistdata.o \
		undistort.o redefine_pointing.o
		ar rv libtwoplane.a two_plane.o distort.o initdistdata.o \
		undistort.o redefine_pointing.o

install:
		cp libtwoplane.a ../..
		cp two_plane.h distort.h ../../include

clean:
		rm -f libtwoplane.a *.o *.a core
