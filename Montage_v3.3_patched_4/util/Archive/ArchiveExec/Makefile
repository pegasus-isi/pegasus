# Filename: Makefile.SunOS

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------
# 1.0      John Good        15Dec04  Original Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../../lib/include
LIBS   =	-L../../../lib -lwcs -lmtbl -lsvc

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mArchiveExec:	mArchiveExec.o
		$(CC) -o mArchiveExec mArchiveExec.o $(LIBS)

install:
		cp mArchiveExec ../../../bin

clean:
		rm -f mArchiveExec *.o
