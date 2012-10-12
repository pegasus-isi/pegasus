# Filename: Makefile.SunOS

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        21Sep07  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include -I../../Montage
LIBS   =	-L../../lib -lsvc -lmtbl -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mTblExec:	mTblExec.o
		$(CC) -o mTblExec mTblExec.o $(LIBS)

install:
		cp mTblExec ../../bin

clean:
		rm -f mTblExec *.o
