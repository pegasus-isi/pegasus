#
# make for LaTeX
#
LATEX	= latex
#PS2PDF	= ps2pdf14
PS2PDF	= ps2pdf -sPAPERSIZE=letter
DVIPS	= dvips
PRTGIF	= tgif -print
RM	= rm -f

%.pdf : %.ps
	$(PS2PDF) $< $@
%.ps : %.dvi
	$(DVIPS) $< -o $@
%.eps : %.obj
	$(PRTGIF) -adobe=3.0/1.2 -eps $< 

all : T2.pdf

#SRC	= euryale.obj partition.obj pegasus.obj sphinx.obj
#EPS	= $(SRC:.obj=.eps)

T2.pdf : T2.ps
T2.ps : T2.dvi
T2.dvi : T2.tex $(EPS)
	$(LATEX) $<
	$(LATEX) $<

clean:
	$(RM) T2.aux T2.log T2.out T2.toc T2.ps

distclean: clean
	$(RM) $(EPS) T2.pdf T2.dvi
