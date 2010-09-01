#!/usr/bin/env perl
#
use 5.006;
use strict;
use warnings; 

use Pegasus::DAX::Factory qw(:all); 

my $xmlns = shift;		# optional argument: XML namespace to use

my $file = newFile( name => 'hw.txt', link => LINK_OUT );
$file->addPFN( newPFN( url => 'hw.txt', site => 'local' ) ); 

my $job = newJob( namespace => 'tut', name => 'echo' ); 
$job->addArgument( 'Hello World' ); 
$job->stdout( $file ); 

my $adag = newADAG( name => 'helloworld' ); 
$adag->addJob( $job ); 
$adag->addFile( $file ); 

use IO::Handle; 		# so that \*STDOUT works
$adag->toXML( \*STDOUT, '', $xmlns ); 
