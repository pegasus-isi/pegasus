CC = gcc -g -I.. -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

all:		test1 test2 test3 structtest abortchild aborttest

test1:		test1.o
		$(CC) -o test1 test1.o -L.. -lsvc

test2:		test2.o
		$(CC) -o test2 test2.o -L.. -lsvc

test3:		test3.o
		$(CC) -o test3 test3.o -L.. -lsvc

structtest:	structtest.o
		$(CC) -o structtest structtest.o -L.. -lsvc

abortchild:	abortchild.o
		$(CC) -o abortchild abortchild.o -L.. -lsvc

aborttest :	aborttest.o
		$(CC) -o aborttest aborttest.o -L.. -lsvc

test1.o:	test1.c
test2.o:	test2.c
test3.o:	test3.c
structtest.o:	structtest.c
abortchild.o:	abortchild.c
aborttest.o:	aborttest.c

clean:
		rm -f *.o test1 test2 test3 structtest
