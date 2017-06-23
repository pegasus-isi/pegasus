Runs a data flow corresponding to the linear_2nodes in examples/direct
directory of a DECAF distribution.

Was tested on obelix.isi.edu by changing the universe for condorpool
site to local instead of vanilla, and manually commenting the
request_cpus key in the generated submit file. this is only required
while trying to run in local universe. should not be required if
running in vanilla universe. 

make sure your PATH is set in the sites.xml that has the right mpirun.
