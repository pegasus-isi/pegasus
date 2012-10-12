all:		test1 test2 test3 tbl2xml test_mtblio nian nian2 multi_test

test_mtblio:		test_mtblio.o ../libmtbl.a
		gcc -g -o test_mtblio test_mtblio.o -L.. -lmtbl

test_mtblio.o:	test_mtblio.c
		gcc -g -I.. -c test_mtblio.c

nian:		nian.o ../libmtbl.a
		gcc -g -o nian nian.o -L.. -lmtbl

nian.o:	nian.c
		gcc -g -I.. -c nian.c

nian2:		nian2.o ../libmtbl.a
		gcc -g -o nian2 nian2.o -L.. -lmtbl

nian2.o:	nian2.c
		gcc -g -I.. -c nian2.c

test1:		test1.o ../libmtbl.a
		gcc -g -o test1 test1.o -L.. -lmtbl

test1.o:	test1.c
		gcc -g -I.. -c test1.c

test2:		test2.o ../libmtbl.a
		gcc -g -o test2 test2.o -L.. -lmtbl

test2.o:	test2.c
		gcc -g -I.. -c test2.c

test3:		test3.o ../libmtbl.a
		gcc -g -o test3 test3.o -L.. -lmtbl

test3.o:	test3.c
		gcc -g -I.. -c test3.c

multi_test:	multi_test.o ../libmtbl.a
		gcc -g -o multi_test multi_test.o -L.. -lmtbl

multi_test.o:	multi_test.c
		gcc -g -I.. -c multi_test.c

tbl2xml:	tbl2xml.o ../libmtbl.a
		gcc -g -o tbl2xml tbl2xml.o -L.. -lmtbl

tbl2xml.o:	tbl2xml.c
		gcc -g -I.. -c tbl2xml.c

clean:
		rm -f test_mtblio test1 test2 test3 tbl2xml *.o core
