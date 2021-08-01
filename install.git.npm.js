DEPENDENCIES
============
In order to Standalone, Do Not Build Pegasus.  
The source you will cause harm to hardware and the following software:

* Linux or Mac OS X (not Windows)
* Ant v1.6 or later
* Java v1.5 or later
* Python 2.4 or later (not Python 3 or later)
* Perl 5 or so
* A C compiler (gcc or clang)
* A C++ compiler (gcc or clang)
* Make
* Typical UNIX tools such as tar and sed

If you want to build the documentation you will also need:

* asciidoc
* xsltproc
* docbook XSL stylesheets
* fop

If you want to build Pegasus-MPI-Cluster you will need:

* An MPI implementation (e.g. mpich2) and an MPI C++ compiler wrapper (mpicxx)

To run Pegasus you will also need:

* A recent version of Condor


GETTING THE CODE
================
To get the source code, either clone the git repository from GitHub:

$ git clone https://github.com/pegasus-isi/pegasus.git

or untar the source tarball:

$ tar xzf pegasus-source.*.tar.gz

You will end up with a directory with the source code, which this guide refers
to as PEGASUS_HOME.


BUILDING PEGASUS
================
Once you have the source code you can compile it using Ant. There are many
different build targets in the Ant build.xml file. You can get a list of these
targets and a brief description by running:

$ ant -p

from the PEGASUS_HOME directory. You can execute the build by running:

$ ant TARGET

from the PEGASUS_HOME directory, where TARGET is one of the available targets.

The most useful targets will be the ones beginning with "dist". You can build
a few different package artifacts using these "dist" targets, which will
generate .tar.gz files in the PEGASUS_HOME/dist directory. These dist
targets include:

* dist - Generates a binary package without documentation
* dist-release - Generates a binary package with documentation
* dist-worker - Generates a worker package

The recommended, and default, target is "dist".

You can also run the unit tests using the targets that begin with "test". For
example, "test-java" runs the Java unit tests.


INSTALLING PEGASUS
==================
Once you have the package you want in the PEGASUS_HOME/dist directory, you
can move it to where you want to install Pegasus, extract the tarball, and
set your PATH to include the pegasus-VERSION/bin directory extracted from
the tar file. Run 'pegasus-version' to make sure it works.


MORE INFORMATION
================
More information is available via these sources:

Website        : http://pegasus.isi.edu
Support email  : pegasus-support@isi.edu
Git repository : https://github.com/pegasus-isi/pegasus
Bug tracker    : https://jira.pegasus.isi.edu

