dag.o: dag.cpp strlib.h dag.h failure.h log.h
engine.o: engine.cpp strlib.h dag.h failure.h log.h engine.h
failure.o: failure.cpp failure.h
log.o: log.cpp log.h
master.o: master.cpp /usr/include/mpi/mpi.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/mpicxx.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/constants.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/exception.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file_inln.h master.h engine.h \
 dag.h failure.h protocol.h log.h
pegasus-mpi-cluster.o: pegasus-mpi-cluster.cpp /usr/include/mpi/mpi.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/mpicxx.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/constants.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/exception.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file_inln.h dag.h engine.h \
 master.h worker.h failure.h log.h
protocol.o: protocol.cpp /usr/include/mpi/mpi.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/mpicxx.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/constants.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/exception.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file_inln.h protocol.h
strlib.o: strlib.cpp strlib.h
test-dag.o: test-dag.cpp dag.h failure.h
test-engine.o: test-engine.cpp dag.h engine.h failure.h
test-log.o: test-log.cpp log.h failure.h
test-strlib.o: test-strlib.cpp strlib.h failure.h
worker.o: worker.cpp strlib.h /usr/include/mpi/mpi.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/mpicxx.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/constants.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/exception.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/datatype_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/functions_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/request_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/comm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intracomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/topology_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/intercomm_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/group_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/op_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/errhandler_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/status_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/info_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/win_inln.h \
 /usr/include/mpi/openmpi/ompi/mpi/cxx/file_inln.h worker.h protocol.h \
 log.h failure.h
