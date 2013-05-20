use 5.006;
use strict;
use IO::Handle;
use Pegasus::DAX::Factory qw(:all);

my $adag = newADAG(name => 'pipeline');

my $webpage = newFilename(name => 'pegasus.html', (link => LINK_OUT, transfer => 'false', register => 'false'));

my $curl = newJob(name => 'curl');
$curl->addArgument('-o', $webpage, 'http://pegasus.isi.edu');
$adag->addJob($curl);

my $count = newFilename(name => 'count.txt', (link=>LINK_OUT, transfer=>'true', register=>'false'));
my $wc = newJob(name => 'wc');
$webpage->link(LINK_IN);
$wc->addArgument('-l', $webpage);
$wc->stdout($count);
$adag->addJob($wc);

$adag->addDependency($curl, $wc);

$adag->toXML(\*STDOUT);

