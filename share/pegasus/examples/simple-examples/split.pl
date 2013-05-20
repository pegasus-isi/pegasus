use 5.006;
use strict;
use IO::Handle;
use Pegasus::DAX::Factory qw(:all);

my $adag = newADAG(name => 'split');

my $webpage = newFilename(name => 'pegasus.html', (link => LINK_OUT, transfer => 'false', register => 'false'));

my $curl = newJob(name => 'curl');
$curl->addArgument('-o', $webpage, 'http://pegasus.isi.edu');
$adag->addJob($curl);

my $split = newJob(name => 'split');
$webpage->link(LINK_IN);
$split->addArgument("-l 100","-a 1",$webpage,"part.");


foreach my $c ('a' .. 'd') {
    my $part = newFilename(name=>"part.".$c, (link=>LINK_OUT, register=>'false', transfer=>'false'));
    $split->addUses($part);

    my $count = newFilename(name=>'count.txt.'.$c, (link=>LINK_OUT, register=>'false', transfer=>'true'));

    my $wc = newJob(name=>'wc');
    $wc->stdout($count);
    $part->link(LINK_IN);
    $wc->addArgument('-l',$part);
    $adag->addJob($wc);

    $adag->addDependency($split, $wc);
}

$adag->addJob($split);
$adag->addDependency($curl, $split);

$adag->toXML(\*STDOUT);

