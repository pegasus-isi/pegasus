# Filename: Makefile.LINUX

# Version  Developer        Date     Change
# -------  ---------------  -------  -----------------------
# 1.0      John Good        20Feb07  Original LINUX Makefile

.SUFFIXES:
.SUFFIXES: .c .o

CC     =	gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CFLAGS =	-g -I. -I./rtree -I../../lib/include -I../../Montage
LIBS   =	-Lrtree -lrtree \
		-L../../lib -lcmd -lwcs -lcoord -lmtbl -lcfitsio -lnsl -lm

.c.o:
		$(CC) $(CFLAGS)  -c  $*.c

mQuickSearch:	mQuickSearch.o
		(cd rtree; make)
		$(CC) -o mQuickSearch mQuickSearch.o \
		../../Montage/debugCheck.o   \
		../../Montage/checkFile.o   \
		../../Montage/checkWCS.o   \
		$(LIBS)

install:
		cp mQuickSearch ../../bin

clean:
		(cd rtree; make clean)
		rm -f mQuickSearch *.o
