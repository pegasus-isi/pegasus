#
# License: (atend)
# $Id$
#
package Pegasus::DAX::AbstractJob;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Exporter;
our @ISA = qw(Pegasus::DAX::Base Exporter); 

use constant INVOKE_NEVER => 'never';
use constant INVOKE_START => 'start'; 
use constant INVOKE_ON_SUCCESS => 'on_success';
use constant INVOKE_ON_ERROR => 'on_error';
use constant INVOKE_AT_END => 'at_end'; 
use constant INVOKE_ALL => 'all'; 

our $VERSION = '3.2'; 
our @EXPORT = (); 
our %EXPORT_TAGS = ( all => [qw(INVOKE_NEVER INVOKE_START INVOKE_ON_SUCCESS
	INVOKE_ON_ERROR INVOKE_AT_END INVOKE_ALL) ] ); 
our @EXPORT_OK = ( @{$EXPORT_TAGS{all}} );

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    $self->{separator} = ' '; 	# between arguments default

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

    # WARNING: foreach is susceptible to in-place modification of the
    # underlying object through the iterator variable!
    my $arg;
    foreach my $name ( @_ ) {
	if ( ! ref $name ) {
	    # plain text -- take as is
	    $arg = "$name"; 	# deep copy
	} elsif ( $name->isa('Pegasus::DAX::PlainFilename')) {
	    # auto-add uses for P::D::Filename
	    $self->uses($name) if $name->isa('Pegasus::DAX::Filename'); 

	    # sub-classing not permissible for storing/printing
	    $arg = Pegasus::DAX::PlainFilename->new( $name->name )
	} elsif ( $name->isa('Pegasus::DAX::CatalogType') ) {
	    # File or Executable
	    $self->uses($name); 
	    $arg = Pegasus::DAX::PlainFilename->new( $name->name ); 
	} else {
	    croak "Illegal argument to addArgument"; 
	}

	if ( exists $self->{arguments} ) { 
	    push( @{$self->{arguments}}, $arg );
	} else {
	    $self->{arguments} = [ $arg ]; 
	}
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

sub stdio($$;@) { 
    my $self = shift;
    my $what = shift;

    my $result = $self->{$what}; 
    if ( @_ ) { 
	my $name = shift; 
	if ( ! ref $name ) { 
	    # plain string
	    $self->{$what} = $name; 
	} elsif ( $name->can('name') ) { 
	    # some class?
	    $self->{$what} = $name->name; 
	    
	    $self->uses($name)
		if ( $name->isa('Pegasus::DAX::Filename') || 
		     $name->isa('Pegasus::DAX::CatalogType') ); 
	} else {
	    croak "illegal name argument";
	}
    }
    $result; 
}

sub stdin {
    my $self = shift;
    stdio($self,'stdin',@_);
}

sub stdout {
    my $self = shift;
    stdio($self,'stdout',@_); 
}

sub stderr {
    my $self = shift;
    stdio($self,'stderr',@_);
}

sub addUses {
    my $self = shift; 
    $self->uses(@_); 
}

sub uses {
    my $self = shift; 
    my $uses = shift; 
    if ( defined $uses && ref $uses ) { 
	if ( $uses->isa('Pegasus::DAX::Filename') ) {
	    $self->{uses}->{ $uses->name } =
		Pegasus::DAX::Filename->new( $uses ); # deep copy!
	} elsif ( $uses->isa('Pegasus::DAX::Executable') ) {
	    $self->{uses}->{ $uses->name } =
		Pegasus::DAX::Filename->new( namespace => $uses->namespace,
					     name => $uses->name,
					     version => $uses->version,
					     executable => 1 );
	} elsif ( $uses->isa('Pegasus::DAX::File') ) { 
	    $self->{uses}->{ $uses->name } =
		Pegasus::DAX::Filename->new( name => $uses->name,
					     link => $uses->link,
					     optional => $uses->optional,
					     executable => 0 ); 
	} else {
	    croak( "Instance of ", ref $uses, ' is an invalid argument' );
	}
    } else {
	croak "invalid argument"; 
    }
}

sub addInvoke {
    my $self = shift;
    $self->invoke(@_);
}

sub notify {
    my $self = shift; 
    $self->invoke(@_);
}

sub invoke {
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
	my $flag = 0; 
	$f->print( "$indent<$tag>" ); 
	foreach my $i ( @{$self->{arguments}} ) {
	    $f->print( $self->{separator} ) if ( $flag && $self->{separator} ); 
	    if ( ref $i ) {
		$i->toXML($f,'',$xmlns); 
	    } else {
		$f->print($i); 
	    }
	    $flag++; 
	}
	$f->print( "</$tag>\n" ); 
    }

    #
    # <profile>
    #
    if ( exists $self->{profiles} ) {
	foreach my $i ( @{$self->{profiles}} ) { 
	    $i->toXML($f,$indent,$xmlns); 
	}
    }

    #
    # <stdio>
    #
    if ( exists $self->{stdin} && $self->{stdin} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stdin" : 'stdin'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$self->stdin,$xmlns)
		 , attribute('link','in',$xmlns)
		 , " />\n" );
    }
    if ( exists $self->{stdout} && $self->{stdout} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stdout" : 'stdout'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$self->stdout,$xmlns)
		 , attribute('link','out',$xmlns)
		 , " />\n" );
    }
    if ( exists $self->{stderr} && $self->{stderr} ) { 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:stderr" : 'stderr'; 
	$f->print( "$indent<$tag"
		 , attribute('name',$self->stderr,$xmlns)
		 , attribute('link','out',$xmlns)
		 , " />\n" );
    }

    #
    # <uses>
    #
    if ( exists $self->{uses} ) { 
	while ( my ($name,$i) = each %{$self->{uses}} ) {
	    $i->toXML($f,$indent,$xmlns);
	}
    }

    #
    # <invoke>
    #
    if ( exists $self->{invokes} ) {
	my $tag = defined $xmlns && $xmlns ? "$xmlns:invoke" : 'invoke';
	foreach my $i ( @{$self->{invokes}} ) {
	    $f->print( "$indent<$tag"
		     , attribute('when',$i->{when},$xmlns)
		     , ">"
		     , quote($i->{cmd})
		     , "</$tag>\n"
		     ); 
	}
    }

}

1; 
__END__



=head1 NAME

Pegasus::DAX::AbstractJob - abstract base class for jobs. 

=head1 SYNOPSIS

This is an abstract class. You do not instantiate abstract classes. 

=head1 DESCRIPTION

This class is the base for the four kinds of jobs and sub-workflows. 

=head1 CONSTANTS

=over 4 

=item INVOKE_NEVER

Never run the invoke. This is primarily to temporarily disable an invoke. 

=item INVOKE_START

Run the invoke when the job gets submitted. 

=item INVOKE_ON_SUCCESS

Run the invoke after the job finishes with success (exitcode == 0). 

=item INVOKE_ON_ERROR

Run the invoke after the job finishes with failure (exitcode != 0). 

=item INVOKE_AT_END

Run the invoke after the job finishes, regardless of exit code. 
 
=item INVOKE_ALL

Like C<INVOKE_START> and C<INVOKE_AT_END> combined. 

=back

=head1 METHODS

=over 4

=item new()

The constructor is used by child classes to establish data structures. 

=item addArgument( $string )

This method will add a simple string into the ordered list of arguments. 

=item addArgument( $plainfilename_instance )

This method adds a simple filename into the ordered list of
arguments. You will have to add the filename separately to the C<uses>
section.

=item addArgument( $filename_instance )

=item addArgument( $file_instance )

=item addArgument( $exectuable_instance )

This method adds a full filename to the ordered list of arguments B<and>
also adds the filename to the C<uses> section.

=item addArgument( ... )

You may pass any number of the above permitted arguments as long list
of these arguments. This is a convenience method. 

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

This method will add a specified profile, either as three strings or
instance of L<Pegasus::DAX::Profile>, to the collection of profiles
associated with the logical level catalog entry. 

=item stdin

=item stdout

=item stderr

Setter and getter for stdio handles. In get mode, the plain string
of the logical file is returned. 

In set mode, use a string or L<Pegasus::DAX::PlainFilename> to provide
the logical file name. You are responsible to add the filename to the
C<uses> section.

You may also specify an argument of L<Pegasus::DAX::Filename>, 
L<Pegasus::DAX::File>, or L<Pegasus::DAX::Executable>. In these cases,
the filename is added automatically to the C<uses> section. You are
responsible to provide the proper linkage, if applicable. 

=item addUses

Alias method for C<uses> method.

=item uses( $filename_instance )

=item uses( $file_instance )

=item uses( $executable_instance )

This method adds a filename, file, or executable to the things that will
end up in the uses section of a job. In case of a L<Pegasus::DAX::Filename>
instance, a deep copy is made so that you can change attributes on your
object. 

=item addInvoke( $when, $cmd )

Alias for C<invoke> method.

=item notify( $when, $cmd ) 

Alias for C<invoke> method.

=item invoke( $when $cmd )

This method adds a simple executable instruction to run (on the submit
host) when a job reaches the state in C<$when>. Please refer to the 
constants C<INVOKE_*> for details. 

=item name

Getter and setter for the job's name required string. Regardless of the
child class, any job always some form of name.

=item id

Getter and setter for the job's identifier string. Please note that the
identifier is more restrictive, e.g. needs to obey more stringend rules.

The job identifier is a required argument, and unique within the C<ADAG>. 

=item nodelabel

Getter and setter for the optional job label string. 

=item separator

This attribute defaults to a single space. The arguments in the argument
string will be formatted with the separator value between each argument. 
The default should be good in many circumstances. 

In case your application is sensitive to white-space in its argument
list, you may want to set C<separator> to the empty string, and provide
the proper whitespaces yourself. 

=item innerXML( $handle, $indent, $xmlns )

The purpose of the C<innerXML> function is to recursively generate XML from
the internal data structures. Since this class is abstract, it will not
create the element tag nor attributes. However, it needs to create the
inner elements as necessary. 

The first argument is a file handle open for writing. This is where the
XML will be generated.  The second argument is a string with the amount
of white-space that should be used to indent elements for pretty
printing. The third argument may not be defined. If defined, all element
tags will be prefixed with this name space.

=back 

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::Base>

Base class. 

=item L<Pegasus::DAX::DAG>

=item L<Pegasus::DAX::DAX>

=item L<Pegasus::DAX::Job>

=item L<Pegasus::DAX::ADAG>

Child classes inheriting from L<Pegasus::DAX::AbstractJob>. 

=back 

=head1 COPYRIGHT AND LICENSE

Copyright 2007-2010 University Of Southern California

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut
