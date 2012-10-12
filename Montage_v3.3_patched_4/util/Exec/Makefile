# Filename: Makefile.LINUX

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        04Oct05  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include -I../../Montage
LIBS   =	-L../../lib -lsvc -lmtbl -lwcs -lcfitsio -lcoord -lnsl -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mExec:		mExec.o
		$(CC) -o mExec mExec.o \
		../../Montage/debugCheck.o   \
		../../Montage/filePath.o   \
		$(LIBS)

install:
		cp mExec ../../bin

clean:
		rm -f mExec *.o
