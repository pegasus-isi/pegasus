# Filename: Makefile.Darwin

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        14Dec04  Original Darwin Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	cc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../../lib/include
LIBS   =	-L../../../lib -lwcs -lcfitsio -lcoord -lmtbl -lsvc \
		-lboundaries -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mArchiveList:	mArchiveList.o
		$(CC) -o mArchiveList mArchiveList.o $(LIBS)

install:
		cp mArchiveList ../../../bin

clean:
		rm -f mArchiveList *.o
