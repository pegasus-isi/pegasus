#
# License: (atend)
# $Id$
#
package Pegasus::DAX::Base;
use 5.006;
use strict;
use vars qw($AUTOLOAD);
use Carp; 

use Exporter;
our @ISA = qw(Exporter); 

sub quote($);			# { }
sub attribute($$);		# { }
sub boolean($);			# { }

our $VERSION = '3.2'; 
our @EXPORT = (); 
our @EXPORT_OK = qw(quote attribute boolean $escape %escape); 
our %EXPORT_TAGS = ( 
    xml => [ @EXPORT_OK ], 
    all => [ @EXPORT_OK ] ); 

our $prefix = '[' . __PACKAGE__ . '] ';

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = bless { @_ }, $class; 

    $self;
}

sub AUTOLOAD {
    # purpose: catch-all accessor (set and get) for all data fields
    #          ever defined in any great-grandchild of this class
    # warning: The autoload maps the data fields XYZ to method XYZ
    # paramtr: ?
    # returns: ?
    my $self = shift;
    my $type = ref($self) or croak( $prefix, "$self is not an object" );

    my $name = $AUTOLOAD;
    $name =~ s/.*:://;   # strip fully-qualified portion

    unless ( exists $self->{$name} || $self->can($name) ) { 
        croak( $prefix, "Can't access >>$name<< field in class $type" );
    }

    my $result = $self->{$name}; 
    if ( ref $self->{$name} eq 'HASH' ) { 
	# hash value
	if ( @_ > 0 ) {
	    if ( ref $_[0] eq 'HASH' && @_ == 1 ) {
		$self->{$name} = { @{shift()} }; # deep copy
	    } elsif ( (@_ & 1) == 0 ) {
		$self->{$name} = { @_ };
	    } else {
		croak( "${type}->${name}() setter is helpless" ); 
	    }
	}

	# return unrolled hash in list context, hashref in scalar
	return wantarray ? ( %{ $result } ) : $result; 
    } elsif ( ref $self->{$name} eq 'ARRAY' ) { 
	# array value
	if ( @_ > 0 ) { 
	    if ( ref $_[0] eq 'ARRAY' && @_ == 1 ) { 
		$self->{$name} = [ @{shift()} ]; # deep copy
	    } else {
		$self->{$name} = [ @_ ]; 
	    }
	}

	# returned unrolled array in list context, arrayref in scalar
	return wantarray ? ( @{ $result } ) : $result;
    } else { 
	# scalar or instance value
	if ( @_ ) {
	    my $v = shift;
	    if ( defined $v ) { $self->{$name} = $v; }
	    else { delete $self->{$name}; }
	}
	return $result; 
    }

    croak "AUTOLOAD: This point should not be reached for ${type}->${name}"; 
}

our %escape = ( '&' => '&amp;'
	      , '<' => '&lt;'
	      , '>' => '&gt;'
	      , "'" => '&apos;'
	      , '"' => '&quot;' 
    );
our $escape = '([' . join( '', keys %escape ) . '])'; 

sub quote($) {
    # purpose: quote XML entities inside a value string
    # paramtr: $s (IN): value string
    # returns: quoted version, possibly same string
    #
    my $s = shift; 
    $s =~ s/$escape/$escape{$1}/ge if defined $s; 
    $s; 
}

sub attribute($$) { 
    # purpose: format an element attribute
    # paramtr: $key (IN): name of attribute
    #          $val (IN): value for attribute
    # returns: formatted string
    # warning: may return empty string if key is empty
    #
    my $key = shift; 
    my $val = shift; 
    if ( defined $key && $key && defined $val ) {
	" $key=\"", quote($val), "\""; 
    } else {
	'';
    }
}

sub boolean($) {
    # purpose: translate perl boolean into xml boolean
    # paramtr: $v (IN): value
    # returns: string 'true' or string 'false' for defined input
    # warning: returns undefined value for undefined input!
    # warning: string "false" input will return 'false', too. 
    #
    my $s = shift;
    if ( defined $s ) { 
	( $s =~ /false/i || ! $s ) ? 'false' : 'true'; 
    } else { 
	undef;
    }
}

sub toXML {
    # purpose: put self onto stream as XML
    # paramtr: F (IN): perl file handle open for writing
    #          ident (IN): indentation level
    #          xmlns (opt. IN): namespace of element, if necessary
    #
    my $self = shift; 
    croak( ref($self),  " called *abstract* ", __PACKAGE__, "::toXML" ); 
}

1; 
__END__

__END__

=head1 NAME

Pegasus::DAX::Base - base class for all ADAG/DAX related classes.

=head1 SYNOPSIS

    use Pegasus::DAX::Base qw(:xml);
    use Exporter;
    our @ISA = qw(Pegasus::DAX::Base Exporter); 

    ...

    sub toXML {
	my $self = shift;
	my $handle = shift;
	my $indent = shift || '';
	my $xmlns = shift; 
	my $tag = defined $xmlns && $xmlns ? "$xmlns:element" : 'element';

	# open tag
	$handle->print( "$indent<$tag",
		      , attribute('key1',$self->{key1})
		      , attribute('key2',boolean($self->{key2}))
		      , ">\n" );

	# child element
	$self->{aggregate}->toXML( $handle, "  $indent", $xmlns );

	# collection of child elements
	foreach my $i ( @{$self->{collection}} ) {
	    $i->toXML( $handle, "  $indent", $xmlns );
	}

	# closing tag
	$handle->print( "$indent</$tag>\n" ); 
    }

=head1 DESCRIPTION

This module implements the base class for all classes related to 
generating DAX files. It provides helper functions to generate XML,
and mandates that non-abstract child classes implement the C<toXML>
method. 

In addition, this class provides an C<AUTOLOAD> method, which in
effect implements the setter and getter for all scalar values in
any child class. 

=head1 FUNCTIONS

The following section defines true functions, not static methods. If you
don't know the difference, you don't need to worry.

=over 4

=item quote($string)

This function replaces all characters in the given input C<$string> that
require to be entity-escaped. The result is a string that is either the
original string, if it did not contain any characters from C<%escape>,
or the string with entity replaced characters. This method will return
C<undef>, if the input string was C<undef>.

=item attribute($key,$value)

This function is a helper for sub-classes that instantiate the abstract
C<toXML> method when printing an element tag. Given the I<$key> for an
element's attribute, and the I<$value> to put with the element, this
method returns the string to be put into the tag assembly.

The result starts with a space, the key as is, the equal sign, a quote
character, the value as result of the C<quote> method, and the closing
quote character.

If the key is not defined or empty, or the value is not defined, the
empty string will be returned. 

=item boolean($v)

This function translates a Perl boolean value into an XML boolean value.
The output is the string C<false>, if the expression evaluates to a Perl
false value I<or> if the input value matches the expression C</false/i>.
Every other value returns the string C<true>. 

As a quirk to accomodate the omission of attributes, an I<undef> input
will generate I<undef> output. 

=back

=head1 METHODS

=over 4

=item toXML( $handle, $indent, $xmlns )

This I<abstract> function will terminate with an error, unless the
child class overrides it. 

The purpose of the C<toXML> function is to recursively generate XML from
the internal data structures. The first argument is a file handle open
for writing. This is where the XML will be generated.  The second
argument is a string with the amount of white-space that should be used
to indent elements for pretty printing. The third argument may not be
defined. If defined, all element tags will be prefixed with this name
space.

=back 

=head1 VARIABLES

=over 4

=item %escape

This variable contains all characters that require an entity escape in
an XML context, and map to the escaped XML entity that the character
should be replaced with. 

The variable is used internally by the C<quote> static method. 

=item $escape

This string is a regular expression that can be used to identify
characters that will require an entity escape in XML context. 

The variable is used internally by the C<quote> static method. 

=back 

=head1 AUTOLOAD

The C<AUTOLOAD> method implement the getter and setter for all scalar
values in any sibling class. While there is some effort to support
non-scalar setters and getters, please do not use that feature (yet). 

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
