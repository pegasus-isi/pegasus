# Filename: Makefile.LINUX

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.1      John Good        01Mar05  Added coord library ref
# 1.0      John Good        25Sep04  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../lib/include -I../../Montage
LIBS   =	-L../../lib -lwcs -lcoord -lcfitsio -ljpeg -lnsl -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mJPEG:		mJPEG.o
		$(CC) -o mJPEG mJPEG.o \
		../../Montage/checkHdr.o   \
		../../Montage/checkWCS.o   \
		$(LIBS)

install:
		cp mJPEG ../../bin

clean:
		rm -f mJPEG *.o
