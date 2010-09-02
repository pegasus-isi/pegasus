#!/usr/bin/env perl
#
use 5.006;
use strict;
use IO::Handle; 

use Pegasus::DAX::Factory qw(:all); 

use constant NS => 'diamond'; 

my $adag = newADAG( name => NS ); 
my $job1 = newJob( namespace => NS, name => 'preprocess', version => '2.0' );
my $job2 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job3 = newJob( namespace => NS, name => 'findrange', version => '2.0' );
my $job4 = newJob( namespace => NS, name => 'analyze', version => '2.0' );

my $file = newFile( name => 'f.a', link => LINK_IN ); 
$file->addPFN( newPFN( url => 'f.a', site => 'local' ) ); 
$adag->addFile($file); 

my %hash = ( link => LINK_OUT, register => 'false', transfer => 'true' ); 
my $fnb1 = newFilename( name => 'f.b1', %hash );
my $fnb2 = newFilename( name => 'f.b2', %hash ); 
$job1->addArgument( '-a', $job1->name, '-T60', '-i', $file,
		    '-o', $fnb1, $fnb2 ); 
$adag->addJob($job1); 

my $fnc1 = newFilename( name => 'f.c1', %hash );
$fnb1->link( LINK_IN ); 
$job3->addArgument( '-a', $job2->name, '-T60', '-i', $fnb1, 
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
