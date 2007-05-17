#!/usr/bin/env perl
require 5.005;
use File::Spec;			# standard module
use File::Basename;		# standard module

@chimera = grep { -x } glob( File::Spec->catfile($ENV{'PEGASUS_HOME'},'lib','*.jar') );
%chimera = map { ( basename($_) => $_ ) } @chimera;
@cp = grep { ! exists $chimera{basename($_)} } split(':',$ENV{CLASSPATH});
print join(':',(@cp,@chimera)), "\n";
