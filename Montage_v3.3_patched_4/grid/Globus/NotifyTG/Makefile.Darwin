# Filename: Makefile.Darwin

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        04Oct05  Original SunOS Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I../../../lib/include -I../../../Montage
LIBS   =	-L../../../lib -lsvc -lmtbl -lwcs -lcfitsio -lcoord -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mNotifyTG:	mNotifyTG.o
		$(CC) -o mNotifyTG mNotifyTG.o \
		$(LIBS)

install:
		cp mNotifyTG ../../../bin

clean:
		rm -f mNotifyTG *.o
