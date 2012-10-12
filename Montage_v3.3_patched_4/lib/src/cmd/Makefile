CC      =		gcc
CFLAGS  =		-c -g -I. -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
LIBNAME =		libcmd.a

SRCS    =		cmd.c

OBJS    =		${SRCS:.c=.o}


libcmd.a:		${OBJS}
			rm -f ${LIBNAME}
			ar q ${LIBNAME} ${OBJS}

${OBJS}:		cmd.h

install:
			cp libcmd.a ../..
			cp cmd.h ../../include

clean:
			rm -f libcmd.a *.o *.a *.i a.out core
