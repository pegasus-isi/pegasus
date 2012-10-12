CC      =		gcc
CFLAGS  =		-c -g -I. -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
LIBNAME =		libboundaries.a

SRCS    =		boundaries.c

OBJS    =		${SRCS:.c=.o}


libboundaries.a:	${OBJS}
			rm -f ${LIBNAME}
			ar q ${LIBNAME} ${OBJS}

${OBJS}:		boundaries.h

install:
			cp libboundaries.a ../..
			cp boundaries.h ../../include

clean:
			rm -f libboundaries.a *.o *.a *.i a.out core
