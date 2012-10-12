# Filename: Makefile.Darwin

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        11May05  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include -I../../Montage
LIBS   =	-L../../lib -lwcs -lcfitsio -lcoord -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mRotate:	mRotate.o
		$(CC) -o mRotate mRotate.o \
		../../Montage/checkHdr.o   \
		../../Montage/checkWCS.o   \
		$(LIBS)

install:
		cp mRotate ../../bin

clean:
		rm -f mRotate *.o
