#!/usr/bin/env perl
#
use 5.006;
use strict;
use IO::Handle; 
use Cwd; 
use File::Spec;
use File::Basename; 

#
# all this to determine PEGASUS_HOME, which we don't set any more.
# and from there derive the PERL5LIB that we need for the DAX API.
# 
BEGIN {
    my $found = dirname(dirname(getcwd()));

    die "FATAL: Sorry, but you need to include Pegasus into your PATH."
	unless ( defined $found && -d $found ); 
    unshift( @INC, $found );
    $ENV{'PEGASUS_HOME'} = dirname(dirname($found))
	unless exists $ENV{'PEGASUS_HOME'}; 
    warn "# found $found\n"; 
}
use Pegasus::DAX::Factory qw(:all); 

use constant NS => 'diamond'; 

my $adag = newADAG( name => NS ); 
$adag->invoke( INVOKE_AT_END, '/bin/date' );
my $job1 = newJob( namespace => NS, name => 'preprocess', version => '2.0' );
my $job2 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job3 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job4 = newJob( namespace => NS, name => 'analyze', version => '2.0' );

$job4->invoke( INVOKE_AT_END, '/bin/true' );

# create "f.a" locally 
my $fn = "f.a"; 
open( F, ">$fn" ) || die "FATAL: Unable to open $fn: $!\n"; 
my @now = gmtime(); 
printf F "%04u-%02u-%02u %02u:%02u:%02uZ\n", 
	$now[5]+1900, $now[4]+1, @now[3,2,1,0]; 
close F; 

my $file = newFile( name => 'f.a', size => '300' );
$file->addPFN( newPFN( url => 'file://' . Cwd::abs_path($fn),
		       site => 'local' ) ); 
$adag->addFile($file); 

if ( exists $ENV{'PEGASUS_HOME'} ) {
    use File::Spec;
    use POSIX (); 
    my $keg = File::Spec->catfile( $ENV{'PEGASUS_HOME'}, 'bin', 'pegasus-keg' ); 
    my @os = POSIX::uname(); 
    $os[2] =~ s/^(\d+(\.\d+(\.\d+)?)?).*/$1/;

    if ( -x $keg ) { 
	my $app1 = newExecutable( namespace => NS, name => 'preprocess', version => '2.0',
				  arch => $os[4], os => lc($^O), osversion => $os[2] ); 
	$app1->addPFN( newPFN( url => "file://$keg", site => 'local' ) );
	$adag->addExecutable($app1); 
	warn "# created executable for keg\n"; 
    }
}

my %hash = ( link => LINK_OUT, register => 'false', transfer => 'true' ); 
my $fna = newFilename( name => $file->name, link => LINK_IN, size => '200' );
my $fnb1 = newFilename( name => 'f.b1', %hash );
my $fnb2 = newFilename( name => 'f.b2', %hash ); 
$job1->addArgument( '-a', $job1->name, '-T60', '-i', $fna,
		    '-o', $fnb1, $fnb2 ); 
$adag->addJob($job1); 

my %hash1 = ( link => LINK_OUT, register => 'false', transfer => 'true', size => '100' );
my $fnc1 = newFilename( name => 'f.c1', %hash1 );
$fnb1->link( LINK_IN ); 
$job2->addArgument( '-a', $job2->name, '-T60', '-i', $fnb1, 
		    '-o', $fnc1 ); 
$adag->addJob($job2);

my $fnc2 = newFilename( name => 'f.c2', %hash );
$fnb2->link( LINK_IN ); 
$job3->addArgument( '-a', $job3->name, '-T60', '-i', $fnb2, 
		    '-o', $fnc2 ); 
$adag->addJob($job3);

# yeah, you can create multiple children for the same parent
# string labels are distinguished from jobs, no problem
$adag->addDependency( $job1, $job2, 'edge1', $job3, 'edge2' ); 

my $fnd = newFilename( name => 'f.d', %hash ); 
$fnc1->link( LINK_IN );
$fnc2->link( LINK_IN ); 
$job4->separator(''); 		# just to show the difference wrt default
$job4->addArgument( '-a ', $job4->name, ' -T60 -i ', $fnc1, ' ', $fnc2, 
		    ' -o ', $fnd );
$adag->addJob($job4);
# this is a convenience function -- easier than overloading addDependency?
$adag->addInverse( $job4, $job2, 'edge3', $job3, 'edge4' );

my $xmlns = shift; 
$adag->toXML( \*STDOUT, '', $xmlns );
