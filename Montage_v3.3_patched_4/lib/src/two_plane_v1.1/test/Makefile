TwoPlaneTest:	TwoPlaneTest.o
		gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -o TwoPlaneTest TwoPlaneTest.o \
		-L../../.. -ltwoplane -lwcs -lcfitsio -lsocket -lnsl -lm

TwoPlaneTest.o:	TwoPlaneTest.c
		gcc -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -I../../../include \
		-c TwoPlaneTest.c 

run:
		TwoPlaneTest in.hdr out.hdr

clean:
		rm -f TwoPlaneTest *.o
