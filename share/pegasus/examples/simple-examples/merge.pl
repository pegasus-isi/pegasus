use 5.006;
use strict;
use IO::Handle;
use Pegasus::DAX::Factory qw(:all);

my $adag = newADAG(name => 'merge');

my @dirs = ('/bin', '/usr/bin', '/usr/local/bin');
my @jobs;
my @files;

foreach my $i (0 .. $#dirs) {
    my $f = newFilename(name=>'bin_'.$i.'.txt', (link=>LINK_OUT, transfer=>'false',register=>'false'));
    push(@files, $f);

    my $dir = $dirs[$i];
    my $ls = newJob(name => 'ls');
    $ls->addArgument('-l',$dir);
    $ls->stdout($f);
    $adag->addJob($ls);
    push(@jobs, $ls);
}

my $cat = newJob(name=>'cat');
my $output = newFilename(name=>'binaries.txt', (link=>LINK_OUT, transfer=>'true', register=>'false'));
$cat->stdout($output);
foreach my $f (@files) {
    $f->link(LINK_IN);
    $cat->addArgument($f);
}
$adag->addJob($cat);

foreach my $j (@jobs) {
    $adag->addDependency($j, $cat);
}

$adag->toXML(\*STDOUT);

