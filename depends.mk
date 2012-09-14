dag.o: dag.cpp strlib.h dag.h failure.h log.h
engine.o: engine.cpp strlib.h dag.h failure.h log.h engine.h
failure.o: failure.cpp failure.h
log.o: log.cpp log.h
master.o: master.cpp master.h engine.h dag.h failure.h protocol.h log.h \
  tools.h
pegasus-mpi-cluster.o: pegasus-mpi-cluster.cpp dag.h engine.h master.h \
  worker.h failure.h log.h protocol.h svn.h
protocol.o: protocol.cpp protocol.h
strlib.o: strlib.cpp strlib.h
test-dag.o: test-dag.cpp dag.h failure.h
test-engine.o: test-engine.cpp dag.h engine.h failure.h
test-log.o: test-log.cpp log.h failure.h
test-strlib.o: test-strlib.cpp strlib.h failure.h
tools.o: tools.cpp tools.h
worker.o: worker.cpp strlib.h worker.h protocol.h log.h failure.h tools.h
