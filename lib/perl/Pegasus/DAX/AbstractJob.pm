#
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# $Id$
#
package Pegasus::DAX::AbstractJob;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = ();
our %EXPORT_TAGS = (); 

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    
    if ( @_ > 1 ) {
	# called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    }

    bless $self, $class; 
}

sub addArgument { 
    my $self = shift; 
    my $name = shift;
    if ( ! ref $name ) {
	# plain text
	$name = Pegasus::DAX::PlainFilename->new( $name ); 
    } elsif ( $name->isa('Pegasus::DAX::PlainFilename')) {
	# auto-add uses for P::D::Filename
	$self->uses($name) if $name->isa('Pegasus::DAX::Filename'); 

	# sub-classing not permissible for storing/printing
	$name = Pegasus::DAX::PlainFilename->new( $name->name )
	    unless ( ref $name eq 'Pegasus::DAX::PlainFilename' ); 
    } else {
	croak "Illegal argument to addArgument"; 
    }

    if ( exists $self->{arguments} ) { 
	push( @{$self->{arguments}}, $name );
    } else {
	$self->{arguments} = [ $name ]; 
    }
}

sub addProfile {
    my $self = shift;

    my $prof; 
    if ( @_ == 3 ) {
	# explicit
	$prof = Pegasus::DAX::Profile->new( shift(), shift(), shift() ); 
    } elsif ( @_ == 1 && ref $_[0] && $_[0]->isa('Pegasus::DAX::Profile') ) {
	$prof = shift; 
    } else {
	croak "argument is not a valid Profile";
    }

    if ( exists $self->{profiles} ) {
	push( @{$self->{profiles}}, $prof );
    } else {
	$self->{profiles} = [ $prof ]; 
    }
}

sub setStdin {
    my $self = shift;
    my $name = shift; 
    if ( ! ref $name ) { 
	# plain string
	$self->{stdin} = $name; 
    } elsif ( $name->can('name') ) { 
	# some class?
	$self->{stdin} = $name->name; 
    } else {
	croak "illegal name argument";
    }
}

sub setStdout {
    my $self = shift;
    my $name = shift; 
    if ( ! ref $name ) { 
	# plain string
	$self->{stdout} = $name; 
    } elsif ( $name->can('name') ) { 
	# some class?
	$self->{stdout} = $name->name; 
    } else {
	croak "illegal name argument";
    }
}

sub setStderr {
    my $self = shift;
    my $name = shift; 
    if ( ! ref $name ) { 
	# plain string
	$self->{stderr} = $name; 
    } elsif ( $name->can('name') ) { 
	# some class?
	$self->{stderr} = $name->name; 
    } else {
	croak "illegal name argument";
    }
}

sub addUses {
    my $self = shift; 
    $self->uses(@_); 
}

sub uses {
    my $self = shift; 
    my $uses = shift; 
    if ( defined $uses && ref $uses && $uses->isa('Pegasus::DAX::Filename') ) {
	$self->{uses}->{ $uses->name } = $uses; 
    } else {
	croak "argument is not a Filename";
    }
}

sub addInvoke {
    my $self = shift; 
    my $when = shift; 
    my $cmd = shift; 

    if ( defined $when && defined $cmd ) { 
	if ( exists $self->{invokes} ) {
	    push( @{$self->{invokes}}, {$when => $cmd} ); 
	} else {
	    $self->{invokes} = [ { $when => $cmd } ]; 
	}
    } else {
	croak "use proper arguments to addInvoke(when,cmdstring)";
    }
}

# forward declarations
sub name;
sub id;
sub nodelabel;
sub stdin;
sub stdout;
sub stderr; 

sub innerXML {
    # purpose: partial XML for common stuff
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (IN): namespace of element, if necessary
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || ''; 
    my $xmlns = shift; 

    #
    # <arguments>
    #
    if ( exists $self->{arguments} ) {
	my $tag = defined $xmlns && $xmlns ? "$xmlns:argument" : 'argument'; 
	$f->print( "$indent<$tag>" ); 
	foreach my $i ( @{$self->{arguments}} ) {
	    if ( ref $i ) {
		$i->toXML($f,'',$xmlns); 
	    } else {
		$f->print($i); 
	    }
	}
	$f->print( "</$tag>\n" ); 
    }

    #
    # <profile>
    #
    if ( exists $self->{profiles} ) {
	foreach my $i ( @{$self->{profiles}} ) { 
	    $i->toXML($f,"  $indent",$xmlns); 
	}
    }

    #
    # <stdio>
    #
    if ( exists $self->{stdin} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stdin" : 'stdin'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$f->stdin)
		 , attribute('link','in')
		 , "/>\n" );
    }
    if ( exists $self->{stdout} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stdout" : 'stdout'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$f->stdout)
		 , attribute('link','out')
		 , "/>\n" );
    }
    if ( exists $self->{stderr} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stderr" : 'stderr'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$f->stderr)
		 , attribute('link','out')
		 , "/>\n" );
    }

    #
    # <uses>
    #
    if ( exists $self->{uses} ) { 
	foreach my $i ( @{$self->{uses}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <invoke>
    #
    if ( exists $self->{invokes} ) {
	my $tag = defined $xmlns && $xmlns ? "$xmlns:invoke" : 'invoke';
	foreach my $i ( @{$self->{invokes}} ) {
	    $f->print( "$indent<$tag"
		     , attribute('when',$i->{when})
		     , ">"
		     , quote($i->{cmd})
		     , "</$tag>\n"
		     ); 
	}
    }

}

1; 
