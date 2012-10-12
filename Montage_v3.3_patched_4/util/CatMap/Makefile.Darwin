# Filename: Makefile.Darwin

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        16Aug05  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include -I../../Montage
LIBS   =	-L../../lib -lmtbl -lcoord -lwcs -lcfitsio -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mCatMap:	mCatMap.o
		$(CC) -o mCatMap mCatMap.o \
		../../Montage/checkHdr.o   \
		../../Montage/checkWCS.o   \
		../../Montage/debugCheck.o \
		$(LIBS)

install:
		cp mCatMap ../../bin

clean:
		rm -f mCatMap *.o
