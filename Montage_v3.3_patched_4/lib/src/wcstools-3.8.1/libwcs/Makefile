CFLAGS= -g
CC= cc

OBJS =	imsetwcs.o imgetwcs.o matchstar.o findstar.o daoread.o wcscon.o \
	fitswcs.o wcsinit.o wcs.o ty2read.o webread.o tmcread.o \
	gscread.o gsc2read.o ujcread.o uacread.o ubcread.o ucacread.o \
	sdssread.o tabread.o binread.o ctgread.o actread.o catutil.o \
	skybotread.o imrotate.o fitsfile.o imhfile.o \
	hget.o hput.o imio.o dateutil.o imutil.o \
	worldpos.o tnxpos.o dsspos.o platepos.o \
	sortstar.o platefit.o iget.o fileutil.o \
	wcslib.o lin.o cel.o proj.o sph.o wcstrig.o distort.o

libwcs.a:	$(OBJS)
	ar rv $@ $?
	ranlib $@

actread.o:	fitsfile.h wcscat.h wcs.h fitshead.h wcslib.h
binread.o:	wcscat.h wcs.h fitshead.h wcslib.h
ctgread.o:	wcscat.h wcs.h fitshead.h wcslib.h
catutil.o:	wcscat.h wcs.h fitshead.h wcslib.h
cel.o:		wcslib.h
daoread.o:	wcscat.h
dateutil.o:	fitsfile.h fitshead.h
distort.o:	wcs.h fitshead.h wcslib.h
dsspos.o:	wcs.h fitshead.h wcslib.h
fileutil.o:	fitsfile.h fitshead.h
imutil.o:	fitsfile.h fitshead.h
findstar.o:	fitshead.h lwcs.h wcscat.h
fitsfile.o:	fitsfile.h fitshead.h
fitswcs.o:	fitsfile.h wcs.h fitshead.h wcslib.h
gscread.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
gsc2read.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
hget.o:		fitshead.h
hput.o:		fitshead.h
iget.o:		fitshead.h
imgetwcs.o:	fitshead.h wcs.h lwcs.h wcslib.h
imhfile.o:	fitsfile.h fitshead.h
imio.o:		fitsfile.h
imsetwcs.o:	fitshead.h wcs.h lwcs.h wcscat.h wcslib.h
imrotate.o:	fitsfile.h
lin.o:		wcslib.h
matchstar.o:	fitshead.h wcs.h lwcs.h wcslib.h wcscat.h
platefit.o:	wcs.h fitshead.h wcslib.h
platepos.o:	wcs.h fitshead.h wcslib.h
proj.o:		wcslib.h
sdssread.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
skybotread.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
sortstar.o:	wcscat.h
sph.o:		wcslib.h
tabread.o:	fitshead.h wcs.h wcslib.h wcscat.h
tmcread.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
tnxpos.o:	wcs.h fitshead.h wcslib.h
ty2read.o:	fitsfile.h wcs.h wcscat.h fitshead.h wcslib.h
uacread.o:	fitshead.h wcs.h wcscat.h wcslib.h
ubcread.o:	fitshead.h wcs.h wcscat.h wcslib.h
ucacread.o:	fitshead.h wcs.h wcscat.h wcslib.h
ujcread.o:	fitshead.h wcs.h wcscat.h wcslib.h
wcs.o:		fitshead.h wcs.h wcslib.h
wcsinit.o:	fitshead.h wcs.h wcslib.h
wcscon.o:	wcs.h fitshead.h wcslib.h
wcslib.o:	wcslib.h
wcstrig.o:	wcslib.h
webread.o:	wcscat.h
worldpos.o:	wcs.h fitshead.h wcslib.h

clean:
	rm -f *.a *.o
