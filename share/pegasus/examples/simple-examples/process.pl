use 5.006;
use strict;
use IO::Handle;
use Pegasus::DAX::Factory qw(:all);

my $adag = newADAG(name => 'process');

my $listing = newFilename(name => 'listing.txt', (link => LINK_OUT, transfer => 'true', register => 'false'));

my $ls = newJob(name => 'ls');
$ls->addArgument('-l /');
$ls->stdout($listing);

$adag->addJob($ls);

$adag->toXML(\*STDOUT);

