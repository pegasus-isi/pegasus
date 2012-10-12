all:		hulltest nsboxtest gnomonic

hulltest:	hulltest.o 
		gcc -o -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 hulltest hulltest.o \
		-L.. -lboundaries -lm

hulltest.o:	hulltest.c
		gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -c -I. -I.. hulltest.c

nsboxtest:	nsboxtest.o 
		gcc -o -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 nsboxtest nsboxtest.o \
		-L.. -lboundaries -lm

nsboxtest.o:	nsboxtest.c
		gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -c -I. -I.. nsboxtest.c

gnomonic:	gnomonic.o 
		gcc -o -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 gnomonic gnomonic.o \
		-L.. -lboundaries -lm

gnomonic.o:	gnomonic.c
		gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -c -I. -I.. gnomonic.c

run:
		hulltest test1.tbl    > hulltest.agra
		nsboxtest test1.tbl   > test1.agra
		nsboxtest test1.tbl 1 > test1v.agra
		nsboxtest test1.tbl 2 > test1c.agra
		nsboxtest test2.tbl   > test2.agra
		nsboxtest test2.tbl 1 > test2v.agra
		nsboxtest test2.tbl 2 > test2c.agra

clean:
		rm -f core hulltest nsboxtest gnomonic *.agra *.o






