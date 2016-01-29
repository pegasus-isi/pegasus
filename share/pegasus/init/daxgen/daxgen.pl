use 5.006;
use warnings;
use strict;
use Pegasus::DAX::Factory qw(:all);

# API Documentation: http://pegasus.isi.edu/documentation

scalar(@ARGV) == 1 or die "Usage: daxgen.py DAXFILE\n";

my $daxfile = $ARGV[0];

my $dax = newADAG(name => '{{name}}');

# TODO Add some jobs
#my $j = newJob(name => 'myexe');
#my $a = newFilename(name => 'a.txt', (link => LINK_IN, transfer => 'true', register => 'false'));
#my $b = newFilename(name => 'b.txt', (link => LINK_OUT, transfer => 'true', register => 'false'));
#my $c = newFilename(name => 'c.txt', (link => LINK_OUT, transfer => 'true', register => 'false'));
#$j->addArgument('-i', $a, '-o', $b, '-o', $c);
#$dax->addJob($j);

# TODO Add some dependencies
#$dax->addDependency($j, $k);

open my $f, '>', $daxfile or die "Unable to open DAXFILE: $!\n";
$dax->toXML($f);
close($f);

