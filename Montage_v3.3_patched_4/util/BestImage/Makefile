# Filename: Makefile

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------
# 1.0      John Good        14Feb05  Original Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include
LIBS   =	-L../../lib -lwcs -lmtbl -lsvc -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mBestImage:	mBestImage.o \
		../../Montage/checkWCS.o \
		../../Montage/filePath.o \
		../../Montage/debugCheck.o
		$(CC) -o mBestImage \
		mBestImage.o \
		../../Montage/checkWCS.o \
		../../Montage/filePath.o \
		../../Montage/debugCheck.o $(LIBS)

install:
		cp mBestImage ../../bin

clean:
		rm -f mBestImage *.o
