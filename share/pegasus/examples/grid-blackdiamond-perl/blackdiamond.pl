#!/usr/bin/env perl
#
use 5.006;
use strict;
use IO::Handle; 
use Cwd; 
use File::Spec;
use File::Basename; 
use Sys::Hostname;
use POSIX ();

BEGIN { 
    eval `pegasus-config --perl-hash`;
    die "Unable to eval pegasus-config: $@" if $@;
}
use Pegasus::DAX::Factory qw(:all); 
use constant NS => 'diamond'; 

my $adag = newADAG( name => NS ); 
my $job1 = newJob( namespace => NS, name => 'preprocess', version => '2.0' );
my $job2 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job3 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job4 = newJob( namespace => NS, name => 'analyze', version => '2.0' );

# create "f.a" locally 
my $fn = "f.a"; 
open( F, ">$fn" ) || die "FATAL: Unable to open $fn: $!\n"; 
my @now = gmtime(); 
printf F "%04u-%02u-%02u %02u:%02u:%02uZ\n", 
	$now[5]+1900, $now[4]+1, @now[3,2,1,0]; 
close F; 

my $file = newFile( name => 'f.a' );
$file->addPFN( newPFN( url => 'file://' . Cwd::abs_path($fn),
		       site => 'local' ) ); 
$adag->addFile($file); 

# follow this path, if we know how to find 'pegasus-keg'
my $keg = File::Spec->catfile( $pegasus{bin}, 'pegasus-keg' ); 
if ( -x $keg ) { 
    my @os = POSIX::uname(); 
    # $os[2] =~ s/^(\d+(\.\d+(\.\d+)?)?).*/$1/;  ## create a proper osversion
    $os[4] =~ s/i.86/x86/;

    # add Executable instances to DAX-included TC. This will only work,
    # if we know how to access the keg executable. HOWEVER, for a grid
    # workflow, these entries are not used, and you need to 
    # [1] install the work tools remotely
    # [2] create a TC with the proper entries
    if ( -x $keg ) { 
	for my $j ( $job1, $job2, $job4 ) { 
	    my $app = newExecutable( namespace => $j->namespace, 
				     name => $j->name, 
				     version => $j->version, 
				     installed => 'false',
				     arch => $os[4], 
				     os => lc($^O) );
	    $app->addProfile( 'globus', 'maxtime', '2' );
	    $app->addProfile( 'dagman', 'RETRY', '3' );
	    $app->addPFN( newPFN( url => "file://$keg", site => 'local' ) );
	    $adag->addExecutable($app); 
	}
    }
} else {
    die "Hmmm, where is pegasus-keg? I thought it was \"$keg\", giving up for now.\n";
}

my %hash = ( link => LINK_OUT, register => 'false', transfer => 'true' ); 
my $fna = newFilename( name => $file->name, link => LINK_IN );
my $fnb1 = newFilename( name => 'f.b1', %hash );
my $fnb2 = newFilename( name => 'f.b2', %hash ); 
$job1->addArgument( '-a', $job1->name, '-T60', '-i', $fna,
		    '-o', $fnb1, $fnb2 ); 
$adag->addJob($job1); 

my $fnc1 = newFilename( name => 'f.c1', %hash );
$fnb1->link( LINK_IN ); 
$job2->addArgument( '-a', $job2->name, '-T60', '-i', $fnb1, 
		    '-o', $fnc1 ); 
$adag->addJob($job2);

my $fnc2 = newFilename( name => 'f.c2', %hash );
$fnb2->link( LINK_IN ); 
$job3->addArgument( '-a', $job3->name, '-T60', '-i', $fnb2, 
		    '-o', $fnc2 ); 
$adag->addJob($job3);
# a convenience function -- you can specify multiple dependents
$adag->addDependency( $job1, $job2, $job3 );

my $fnd = newFilename( name => 'f.d', %hash ); 
$fnc1->link( LINK_IN );
$fnc2->link( LINK_IN ); 
$job4->separator(''); 		# just to show the difference wrt default
$job4->addArgument( '-a ', $job4->name, ' -T60 -i ', $fnc1, ' ', $fnc2, 
		    ' -o ', $fnd );
$adag->addJob($job4);
# this is a convenience function adding parents to a child. 
# it is clearer than overloading addDependency
$adag->addInverse( $job4, $job2, $job3 );

# workflow level notification in case of failure
# refer to Pegasus::DAX::Invoke for details
my $user = $ENV{USER} || $ENV{LOGNAME} || scalar getpwuid($>); 
$adag->invoke( INVOKE_ON_ERROR, 
	       "/bin/mailx -s 'blackdiamond failed' $user" ); 

my $xmlns = shift; 
$adag->toXML( \*STDOUT, '', $xmlns );
