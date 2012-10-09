dag.o: dag.cpp strlib.h dag.h failure.h log.h
engine.o: engine.cpp strlib.h dag.h failure.h log.h engine.h
failure.o: failure.cpp failure.h
fdcache.o: fdcache.cpp fdcache.h log.h failure.h tools.h
log.o: log.cpp log.h
master.o: master.cpp master.h engine.h dag.h protocol.h comm.h fdcache.h \
  failure.h log.h tools.h
mpicomm.o: mpicomm.cpp mpicomm.h comm.h protocol.h failure.h
pegasus-mpi-cluster.o: pegasus-mpi-cluster.cpp svn.h dag.h engine.h \
  master.h protocol.h comm.h fdcache.h worker.h failure.h log.h mpicomm.h \
  tools.h
protocol.o: protocol.cpp tools.h protocol.h failure.h log.h
strlib.o: strlib.cpp strlib.h
test-dag.o: test-dag.cpp dag.h failure.h log.h
test-engine.o: test-engine.cpp dag.h engine.h failure.h
test-fdcache.o: test-fdcache.cpp fdcache.h failure.h log.h
test-log.o: test-log.cpp log.h failure.h
test-protocol.o: test-protocol.cpp protocol.h failure.h log.h
test-strlib.o: test-strlib.cpp strlib.h failure.h
test-tools.o: test-tools.cpp tools.h
tools.o: tools.cpp tools.h failure.h log.h
worker.o: worker.cpp strlib.h worker.h comm.h protocol.h log.h failure.h \
  tools.h
