CC      =	gcc
CFLAGS  =	-g -c -D_LARGEFILE_SOURCE \
		-D_FILE_OFFSET_BITS=64 -I. -I../../include
LIBNAME =	libcoord.a

SRCS    =	ccalc.c                    \
   		parseCoordinateString.c    \
		sexToDecimalDegree.c       \
		decimalDegreeToSex.c       \
		convertCoordinates.c       \
		convertBesselianJulian.c   \
		precessBesselian.c         \
		precessJulian.c            \
		convertEclEqu.c            \
		convertEquGal.c            \
		convertGalSgal.c           \
		computeFKCorrections.c

OBJS    =	${SRCS:.c=.o}


libcoord.a:	${OBJS}
		rm -f ${LIBNAME}
		ar q ${LIBNAME} ${OBJS}

${OBJS}:	coord.h

install:
		cp libcoord.a ../..
		cp coord.h ../../include

clean:
		rm -f libcoord.a *.o *.a *.i a.out core
