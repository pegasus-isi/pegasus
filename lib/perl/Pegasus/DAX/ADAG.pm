#
# License: (atend)
# $Id$
#
package Pegasus::DAX::ADAG;
use 5.006;
use strict;
use Carp; 

use Pegasus::DAX::Base qw(:xml); 
use Pegasus::DAX::AbstractJob; 
use Exporter;
our @ISA = qw(Pegasus::DAX::AbstractJob Exporter); 

use constant SCHEMA_NAMESPACE => 'http://pegasus.isi.edu/schema/DAX'; 
use constant SCHEMA_LOCATION => 'http://pegasus.isi.edu/schema/dax-3.2.xsd';
use constant SCHEMA_VERSION => 3.2;

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = qw(SCHEMA_NAMESPACE SCHEMA_LOCATION SCHEMA_VERSION); 
our %EXPORT_TAGS = ( 
    schema => [ @EXPORT_OK ], 
    all => [ @EXPORT_OK ] ); 

# one AUTOLOAD to rule them all
BEGIN { *AUTOLOAD = \&Pegasus::DAX::Base::AUTOLOAD }

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new();
    $self->{index} = 0;
    $self->{count} = 1; 
    $self->{version} = SCHEMA_VERSION; 

    if ( @_ > 1 ) {
	# called with a=>b,c=>d list
	%{$self} = ( %{$self}, @_ ); 
    } elsif ( @_ == 1 && ref $_[0] eq 'HASH' ) { 
	# called with { a=>b, c=>d } hashref
	%{$self} = ( %{$self}, %{ shift() } ); 
    }

    bless $self, $class; 
}

# forward declarations
sub name;
sub index;
sub count; 

use Pegasus::DAX::File; 

sub addFile {
    my $self = shift; 
    my $name = shift; 
    if ( ref $name ) {
	if ( $name->isa('Pegasus::DAX::File') ) {
	    push( @{$self->{files}}, $name ); 
	} else { 
	    croak "Instance of ", ref($name), " is an invalid argument"; 
	}
    } else {
	croak "invalid argument"; 
    }
}

sub addExecutable { 
    my $self = shift; 
    my $name = shift; 
    if ( ref $name ) {
	if ( $name->isa('Pegasus::DAX::Executable') ) {
	    push( @{$self->{executables}}, $name ); 
	} else { 
	    croak "Instance of ", ref($name), " is an invalid argument"; 
	}
    } else {
	croak "invalid argument";
    }
}

sub addTransformation { 
    my $self = shift; 
    my $name = shift; 
    if ( ref $name && $name->isa('Pegasus::DAX::Transformation') ) {
	push( @{$self->{transformations}}, $name ); 
    } else { 
	croak "Instance of ", ref($name), " is an invalid argument"; 
    }
}

sub addJob { 
    my $self = shift; 
    my $name = shift; 
    if ( ref $name && $name->isa('Pegasus::DAX::AbstractJob') ) {
	push( @{$self->{jobs}}, $name ); 
    } else { 
	croak "Instance of ", ref($name), " is an invalid argument"; 
    }
}

sub addDependency {
    my $self = shift; 
    my $parent = shift;
    my $child = shift; 
    my $label = shift; 

    # we only need the job identifier string
    if ( ref $parent ) {
	if ( $parent->isa('Pegasus::DAX::AbstractJob') ) {
	    $parent = $parent->id;
	    croak( "parent does not have a valid job-id" )
		unless ( defined $parent && $parent ); 
	} else {
	    croak "parent is not a job type";
	}
    }

    # we only need the job identifier string
    if ( ref $child ) {
	if ( $child->isa('Pegasus::DAX::AbstractJob') ) {
	    $child = $child->id;
	    croak( "child does not have a valid job-id" )
		unless ( defined $child && $child ); 
	} else {
	    croak "child is not a job type"; 
	}
    }

    # spring into existence -- store undef, if necessary
    $self->{deps}->{$child}->{$parent} = $label; 
}

sub toXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (opt. IN): namespace of element, if necessary
    #
    my $self = shift; 
    my $f = shift; 
    my $indent = shift || '';
    my $xmlns = shift; 
    my $tag = defined $xmlns && $xmlns ? "$xmlns:adag" : 'adag';

    # OK, this is slightly ugly and tricky: If there is no indentation,
    # this <adag> element is the outer-most, and thus gets the XML intro.
    if ( $indent eq '' ) { 
	use POSIX qw(strftime); 
	binmode($f,':utf8'); 	# evil, evil, evil
	$f->print( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" ); 
	$f->print( '<!-- generated: ', strftime( "%Y-%m-%dT%H:%M:%S%z", localtime() ), " -->\n" ); 
    }

    my $ns = defined $xmlns && $xmlns ? "xmlns:$xmlns" : 'xmlns'; 
    $f->print( "$indent<$tag"
	     , attribute($ns,SCHEMA_NAMESPACE)
	     , attribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance')
	     , attribute('xsi:schemaLocation',SCHEMA_NAMESPACE . ' ' . SCHEMA_LOCATION)
	     , attribute('version',SCHEMA_VERSION,$xmlns)
	     , attribute('name',$self->name,$xmlns)
	     , attribute('index',$self->index,$xmlns)
	     , attribute('count',$self->count,$xmlns)
	     , ">\n" ); 

    #
    # <file>
    #
    if ( exists $self->{files} ) {
	$f->print( "  $indent<!-- part 1.1: included replica catalog -->\n" );
	foreach my $i ( @{$self->{files}} ) {
	    $i->toXML($f,"  $indent",$xmlns); 
	}
    }

    #
    # <executable>
    #
    if ( exists $self->{executables} ) { 
	$f->print( "  $indent<!-- part 1.2: included transformation catalog -->\n" );
	foreach my $i ( @{$self->{executables}} ) { 
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <transformation>
    #
    if ( exists $self->{transformations} ) { 
	$f->print( "  $indent<!-- part 1.3: included transformation abbreviations -->\n" );
	foreach my $i ( @{$self->{transformations}} ) {
	    $i->toXML($f,"  $indent",$xmlns);
	}
    }

    #
    # <DAG|DAX|Job|ADAG>
    #
    $f->print( "  $indent<!-- part 2: definition of all jobs (at least one) -->\n" ); 
    foreach my $i ( @{$self->{jobs}} ) {
	$i->toXML($f,"  $indent",$xmlns);
    }
    
    #
    # <child>
    # 
    if ( exists $self->{deps} ) {
	$f->print( "  $indent<!-- part 3: list of control-flow dependencies -->\n" ); 
	my $ctag = defined $xmlns && $xmlns ? "$xmlns:child" : 'child';
	my $ptag = defined $xmlns && $xmlns ? "$xmlns:parent" : 'parent';
	while ( my ($child,$r) = each %{$self->{deps}} ) { 
	    $f->print( "  $indent<$ctag"
		     , attribute('ref',$child,$xmlns)
		     , ">\n" );
	    while ( my ($parent,$label) = each %{$r} ) {
		$f->print( "    $indent<$ptag"
			 , attribute('ref',$parent,$xmlns)
			 , attribute('edge-label',$label,$xmlns)
			 , " />\n" ); 
	    }
	    $f->print( "  $indent</$ctag>\n" ); 
	}
    }

    $f->print( "$indent</$tag>\n" ); 
}

1; 
__END__


=head1 NAME

Pegasus::DAX::ADAG - Pegasus workflow description. 

=head1 SYNOPSIS

    use Pegasus::DAX::ADAG; 

    my $d = Pegasus::DAX::ADAG->new( name => 'fubar' ); 
    $d->addJob( $job );
    $d->addDependency( $parent, $child, 'label' ); 

=head1 DESCRIPTION

This class stores the entire abstract directed acyclic graph (ADAG) that
is a Pegasus workflow ready to be planned out. The heavy lifting is done
in the base class L<Pegasus::DAX::AbstractJob>.

Please note that, even though the schema and API permit it, you cannot
stores an C<ADAG> within an C<ADAG>. We are hoping to add recursion at
some unspecified time in the future. 

=head1 METHODS

=over 4

=item new()

=item new( a => b, c => d, ... )

=item new( { a => b, c => d, ... } )

The default constructor will create an empty instance whose scalar
attributes can be adjusted using the getters and setters provided by the
C<AUTOLOAD> inherited method.

Other means of construction is to use named lists.

=item index

Getter and setter to the slot number, starting with 0, of this workflow
variation. This is mostly an obsolete feature that will go away.

=item count

Getter and setter to the total number of slots, a count, of all variations
of this workflow. This is mostly an obsolete feature that will go away.

=item addFile( $file_instance )

Adds an included replica catalog entry. 

=item addExecutable( $executable_instance )

Adds an included transformation catalog entry. 

=item addTransformation( $transformation_instance )

Adds a L<Pegasus::DAX::Transformation> combiner to the workflow. 

=item addJob( $dag_instance )

Adds an already concretized sub-workflow as node to the workflow graph. 

=item addJob( $dax_instance )

Adds a yet to be planned sub-workflow as node to the workflow graph. 

=item addJob( $job_instance )

Adds a regular job as node to the workflow graph. 

=item addJob( $adag_instance )

While not forbidden by the API, we cannot plan C<ADAG> within C<ADAG> yet. 

=item addDependency( $parent, $child )

=item addDependency( $parent, $child, $label )

This method adds a child to the parent, using each job's C<id>
attribute. In addition, an optional edge label may be stored with each
dependency. Internal structures ensure that each relationship is only
added once. 

=item toXML( $handle, $indent, $xmlns )

The purpose of the C<toXML> function is to recursively generate XML from
the internal data structures. The first argument is a file handle open
for writing. This is where the XML will be generated.  The second
argument is a string with the amount of white-space that should be used
to indent elements for pretty printing. The third argument may not be
defined. If defined, all element tags will be prefixed with this name
space.

=back 

=head1 INHERITED METHODS

Please refer to L<Pegasus::DAX::AbstractJob> for inherited methods. 

=over 4

=item addArgument( $string )

=item addArgument( $plainfilename_instance )

=item addArgument( $filename_instance )

=item addArgument( $file_instance )

=item addArgument( $exectuable_instance )

=item addProfile( $namespace, $key, $value )

=item addProfile( $profile_instance )

=item stdin

=item stdout

=item stderr

=item name

=item id

=item nodelabel

=item addUses( .. )

=item uses( $filename_instance )

=item uses( $file_instance )

=item uses( $executable_instance )

=item addInvoke( $when, $cmd )

=item notify( $when, $cmd ) 

=item invoke( $when $cmd )

=item innerXML( $handle, $indent, $xmlns )

=back

=head1 SEE ALSO

=over 4

=item L<Pegasus::DAX::AbstractJob>

Base class. 

=item L<Pegasus::DAX::DAG>

=item L<Pegasus::DAX::DAX>

=item L<Pegasus::DAX::Job>

Sibling classes. 

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
