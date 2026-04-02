job.o: job.c job.h
mysystem.o: mysystem.c tools.h report.h mysystem.h
parser.o: parser.c parser.h tools.h
pegasus-cluster.o: pegasus-cluster.c tools.h parser.h report.h mysystem.h \
 job.h statinfo.h
report.o: report.c tools.h report.h
statinfo.o: statinfo.c statinfo.h
tools.o: tools.c tools.h
try-cpus.o: try-cpus.c
