#!/bin/env perl
#
# converts the dependencies in an ant build file into graphviz format
# $Id: build.pl,v 1.2 2005/05/20 15:54:54 griphyn Exp $
#
use 5.005;
use warnings;
use diagnostics;
use strict;
use XML::Parser::Expat;

my $buildfn = shift || 'build.xml';
my $parser = new XML::Parser::Expat() || 
    die "ERROR: Unable to instantiate XML parser";

print 'Digraph DAG {', "\n";
print '  size="16.0,11.0"', "\n";
print '  ratio = fill', "\n";
print '  node [shape=rectangle, color=orange, style=filled]', "\n";

my (%result,@stack,@deps,$name) = ();
$parser->setHandlers( 'Start' => sub {
        my $self = shift;
        my $element = shift;
        my %attr = @_;
	if ( $element eq 'target' ) {
	    # <target name="xxx" depends="y1,y2..." ...>
	    push( @stack, $attr{name} );
	    $name = '"' . $attr{name} . '"';
	    print "  $name\n";

	    if ( exists $attr{depends} ) {
		foreach my $dep ( split /,/, $attr{depends} ) {
		    print "    $name->\"$dep\"\n";
		}
	    }
	} elsif ( $element eq 'antcall' ) {
	    # <antcall target="y"/>
	    $name = '"' . $stack[$#stack] . '"';
	    print "    $name->\"", $attr{target}, "\" [ color=blue ]\n";
	}
    }, 'End' => sub {
        my $self = shift;
        my $element = shift;
        pop(@stack) if $element eq 'target';
    } );
$parser->parsefile($buildfn);
print "}\n";
