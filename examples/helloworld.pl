#!/usr/bin/env perl
#
use 5.006;
use strict;
use warnings; 

use Pegasus::DAX::ADAG; 
use Pegasus::DAX::Job; 
use Pegasus::DAX::Filename;

my $xmlns = shift;

my $stdout = Pegasus::DAX::Filename->new( name => 'hw.txt',
					  link => LINK_OUT,
					  register => 0,
					  transfer => TRANSFER_TRUE ); 

my $job = Pegasus::DAX::Job->new( namespace => 'tut', name => 'echo' ); 
$job->addArgument( 'Hello World' ); 
$job->stdout( $stdout ); 

my $adag = Pegasus::DAX::ADAG->new( name => 'helloworld' ); 
$adag->addJob( $job ); 

use IO::Handle; 		# so that \*STDOUT works
$adag->toXML( \*STDOUT, '', $xmlns ); 
