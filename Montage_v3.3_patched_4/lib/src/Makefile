all:
	(cd cfitsio-3.25; ./configure; make; cp libcfitsio.a ../..; cp *.h ../../include)
	(cd cmd; make; make install)
	(cd coord; make; make install)
	(cd mtbl; make; make install)
	(cd svc; make; make install)
	(cd boundaries; make; make install)
	(cd pixbounds; make; make install)
	(cd wcstools-3.8.1/libwcs; make; cp libwcs.a ../../..; cp *.h ../../../include)
	(cd two_plane_v1.1; make; make install)
	(cd jpeg-8b; ./configure; make; cp .libs/libjpeg.a ../..; cp jpeglib.h jconfig.h jmorecfg.h ../../include)
	(cd ..; ranlib *.a)
	chmod 644 ../include/*

clean:
	rm -f ../include/* ../*.a
	(cd cfitsio-3.25; make clean; rm -f config.log config.cache config.status)
	(cd cmd; make clean)
	(cd coord; make clean)
	(cd mtbl; make clean)
	(cd svc; make clean)
	(cd boundaries; make clean)
	(cd pixbounds; make clean)
	(cd wcstools-3.8.1/libwcs; make clean)
	(cd jpeg-8b; make clean)
	(cd two_plane_v1.1; make clean)
