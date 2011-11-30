#!/usr/bin/env perl
#
# creation script for nested workflows of multiple levels
# $Id$
#
use 5.006;
use strict;
use IO::Handle; 
use Cwd; 
use File::Spec;
use File::Basename; 
use POSIX (); 

BEGIN { 
    eval `pegasus-config --perl-hash`;
    die "Unable to eval pegasus-config: $@" if $@;
}
use Pegasus::DAX::Factory qw(:all); 
use Pegasus::Common qw(find_exec); 

my $depth = shift || die "Usage: $0 depths [sleeptime]";
my $sleep = shift || 600; 	# 10 minutes for now

my %hash = ( namespace => 'deepthought' ); 
my $keg = find_exec( 'pegasus-keg', $pegasus{bin} ); 
die "FATAL: Unable to find a \'pegasus-keg\'\n" unless defined $keg; 

my @os = POSIX::uname(); 
$os[2] =~ s/^(\d+(\.\d+(\.\d+)?)?).*/$1/;
$os[4] =~ s/i.86/x86/;

sub add_keg_job($) {
    my $adag = shift; 

    my $app = newExecutable( %hash
			   , name => 'sleep'
			   , installed => 'true'
			   , arch => $os[4]
			   , os => lc($^O)
			   );
    $app->addProfile( 'dagman', 'RETRY', '3' );
    $app->addProfile( 'dagman', 'POST.SCOPE', 'all' ); 
    $app->addPFN( newPFN( url => "file://$keg", site => 'local' ) );
    $adag->addExecutable($app); 

    my $job = newJob( %hash, name => 'sleep' ); 
    $job->addArgument( '-t', $sleep );
    $adag->addJob($job); 
}

#
# level=1: always there
#
my $adag = newADAG( name => 'level-1' ); 
add_keg_job($adag); 
open( OUT, ">level-1.dax" ) || die "open level-1.dax: $!\n"; 
$adag->toXML( \*OUT, '', undef ); 
close OUT; 

my ($dax); 
for ( my $level=2; $level <= $depth; ++$level ) { 
    $adag = newADAG( name => "level-$level" );

    my $fn = 'level-' . ($level-1) . '.dax'; 
    $dax = newDAX( %hash, file => $fn ); 
    $dax->addArgument( '--sites', 'local',
		       '-vvv',
		       '--nocleanup',
		       '--force',
		       '--output', 'local' ); 
    $adag->addJob($dax); 

    my $file = newFile( name => $fn );
    $file->addPFN( newPFN( url => 'file://' . Cwd::abs_path($fn)
			 , site => 'local' ) );
    $adag->addFile($file); 

    add_keg_job($adag); 

    open( OUT, ">level-$level.dax" ) || die "open $fn: level-$level.dax: $!\n";
    $adag->toXML( \*OUT, '', undef ); 
    close OUT; 
}
