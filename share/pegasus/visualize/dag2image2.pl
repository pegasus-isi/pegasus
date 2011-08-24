#!/usr/bin/env perl
#
# Convert DAG into image representing the workflow
#
use 5.006;
use strict;
use warnings;
use Getopt::Long qw(:config bundling no_ignore_case);
use File::Temp ();
use File::Spec;

my $image = '';
my $verbose = 0;
my @relations = ();
my @jobs = ();
my @postjobs = ();
my @nodecolors= qw(blueviolet mediumvioletred violet indigo violetred4);
my $size=1;

my $nodeshape = 'circle';
my $iptxnodecolor = 'skyblue';
my $optxnodecolor = 'gold';
my $registernodecolor = 'orange';
my $nodecolor = 'blueviolet';
my $internodecolor = 'limegreen';
my $cleanupnodecolor = 'red';
my $label = 'none';
my $arrowhead = 'normal';
my $arrowsize = '1.0';
my $type = 'jpeg';
my $keep = 0;

sub usage {
    print STDERR "\n";
    print STDERR "Usage: dag2image -o <imagefile> [options] dagfile\n";
    print STDERR "       dag2image --output <imagefile> [options] dagfile\n";
    print STDERR "\n";
    print STDERR "MANDATORY ARGUMENTS:\n";
    print STDERR " dagfile  path to the .dag file\n";
    print STDERR "\n";
    print STDERR "OPTIONAL ARGUMENTS:\n";
    print STDERR " -o|--output <filename>\n";
    print STDERR "\tWhere to put the produced graphic, default is stdout\n";
    print STDERR "\tUse of this option is highly recommended.\n";
    print STDERR " -l|--label  <labelstyle>\n";
    print STDERR "\tDefault is none (verbose|none|info)\n";
    print STDERR " -a|--arrow <arrow head style>\n";
    print STDERR "\tDefault is normal (normal|dot|invdot|open|invnormal)\n";
    print STDERR " -s|--shape <node shape>\n";
    print STDERR "\tDefault is circle (circle|ellipse|doublecircle|point|square|traingle)\n";
    print STDERR " --iptxcolor <ip transfer node color>\n";
    print STDERR "\tDefault is skyblue. Check graphviz documentation for more.\n";
    print STDERR " --optxcolor, see iptxcolor\n";
    print STDERR " --nodecolor, see iptxcolor\n";
    print STDERR " --regcolor, see iptxcolor\n";
    print STDERR " --intertxcolor, see iptxcolor\n";
    print STDERR " -t|--type Type of output jpeg png etc, default jpeg\n";
    print STDERR " --size Restrict the size 1 = 10x8, 2=17x11 , 3 = unrestricted\n";
    print STDERR " --keep if specified, do not remove the dot file\n";
    print STDERR "\n";
    exit(1);
}

my $result = 
    GetOptions ( "output|o=s"      =>\$image,
		 "label|l=s"       =>\$label,
		 "intertxcolor|x=s"=>\$internodecolor,
		 "ahead=s"         =>\$arrowhead,
		 "shape|s=s"       =>\$nodeshape,
		 "iptxcolor=s"   =>\$iptxnodecolor,
		 "optxcolor=s"   =>\$optxnodecolor,
		 "nodecolor=s"   =>\$nodecolor, 
		 "regcolor=s"    =>\$registernodecolor,
		 "verbose|v"       =>\$verbose,
		 "asize|a=f"       =>\$arrowsize,
		 "type|t=s"        =>\$type,
		 "size=i"          =>\$size,
		 "keep!"           => \$keep,
		 "help|h"          =>\&usage);

my $dagfile = shift || die "ERROR: The .dag file is a mandatory argument, see --help\n";
die "ERROR: $dagfile does not exist\n" unless -e $dagfile;
die "ERROR: $dagfile not readable\n" unless -r _;

my $tmp = $ENV{TMPDIR} || $ENV{TMP} || $ENV{TEMP} || File::Spec->tmpdir() || '/tmp';
my $dot = new File::Temp( TEMPLATE => 'dotXXXXXX', DIR => $tmp, SUFFIX => '.dot',
			  UNLINK => (! $keep) );
my $dotfile = $dot->filename;
open( DAG, "<$dagfile" ) || die "ERROR: Unable to open dag file $dagfile $!\n";
open( DOT, ">$dotfile" ) || die "ERROR: Unable to write to dotfile $dotfile $!\n";

# set up counter for statistics
my %count = ( dep => 0, job => 0, post => 0, cleanup=> 0, 
	      stagein => 0, stageout => 0, register => 0, exe => 0, inter => 0 );

print DOT "digraph E {\n";
if ( $size==2 ) {
    print DOT " size=\"17.0,11.0\"\n ratio=auto\n";
} elsif ( $size==1 ) {
    print DOT " size=\"11.5,10.0\"\n ratio=auto\n";
} 
print DOT " node [shape=$nodeshape]\n";
print DOT " edge [arrowhead=$arrowhead, arrowsize=$arrowsize]\n";

while ( <DAG> ) {
    next if /^\#/;		# skip comments
    s/^\s+//; 			# remove leading whitespace
    s/[ \r\n\t]+$//;		# remove trailing whitespace including CRLF
   
    if ( /^PARENT\s/i && /\sCHILD\s/i ) {
	s/^PARENT\s+//i;
	my ($parents,$children) = split /\s+CHILD\s+/i, $_, 2;
	foreach my $parent ( split( /\s+/, $parents ) ) {
	    foreach my $child ( split( /\s+/, $children ) ) {
		# one line per link
		my $what = "$parent -> $child";
		$relations[$count{dep}] = $what;
		$count{dep}++;
		print DOT " $what\n";
		print STDERR "Adding arc $what\n" if $verbose;
	    }
	}
    } elsif ( /^JOB\s/i ) {
	# special job processing
	my ($templabel, $tempstring) = '';
	my $job = $jobs[$count{job}] = (split)[1];
	if ( $job =~ /^rc_tx/ || $job =~ /^stage_in/ ) {
	    $tempstring = " $job [color=$iptxnodecolor, style=filled";
	 
	    if ( $label =~ /info/i ) {
		$templabel="IP-TX-$count{stagein}";
	    } elsif ( $label =~  /verbose/i ) {
		$templabel="$job";
	    }
	    $count{stagein}++;
	} elsif ( $job =~ /^new_rc_tx/ || $job =~ /^stage_out/ ) {
	    $tempstring = " $job [color=$optxnodecolor, style=filled";

	    if ( $label =~ /info/i ) {
		$templabel="OP-TX-$count{stageout}";
	    } elsif ( $label =~ /verbose/i ) {
		$templabel="$job";
	    }
	    $count{stageout}++;
	} elsif ( $job =~ /^new_rc_register/ || $job =~ /^register/ ) {
	    $tempstring = " $job [color=$registernodecolor, style=filled";

	    if ( $label =~ /info/i ) {
		$templabel="RLS_REG-$count{register}";
	    } elsif ( $label =~ /verbose/i ) {
		$templabel="$job";
	    }
	    $count{register}++;
	} elsif ( $job =~ /^inter_tx/ ) {
	    $tempstring = " $job [color=$internodecolor, style=filled";

	    if ( $label =~ /info/i ) {
		$templabel="INTER-TX-$count{inter}";
	    } elsif ( $label =~ /verbose/i ) {
		$templabel="$job";
	    }
	    $count{inter}++;
	} elsif ( $job =~ /^clean_up/ ) {
	    $tempstring = " $job [color=$cleanupnodecolor, style=filled";

	    if ( $label =~ /info/i ) {
		$templabel="CLEANUP-$count{cleanup}";
	    } elsif ( $label =~ /verbose/i ) {
		$templabel="$job";
	    }
	    $count{cleanup}++;
	} 
	else {
	    $tempstring = " $job [color=$nodecolor, style=filled";

	    if ( $label =~ /info/i ) {
		$templabel="Exec-$count{exe}";
	    } elsif ( $label =~ /verbose/i ) {
		$templabel="$job";
	    }
	    $count{exe}++;
	}
	print DOT $tempstring, ', label="', $templabel, "\"]\n";
	print STDERR "Adding node $job\n" if $verbose;
	$count{job}++;
    } elsif ( /^POSTJOB\s/i ) {
	$postjobs[$count{post}]=$_;
	$count{post}++;
    }
}
print STDERR "$count{job} jobs, $count{dep} dependencies, $count{post} post scripts\n";
close DAG;

print DOT "}\n";
close DOT;
print STDERR "Written dot file $dotfile\n";

print STDERR "Generating Image...\n";
my $command="dot -Gconcentrate";
if ( $type eq 'eps' ) {
    # eps is not supported by dot. 
    # This is evil trickery to generate LaTeX figures ;-P
    $command .= " -Tps2 $dotfile";
    $command .= ' | perl -pe \'$_="%!PS-Adobe-3.0 EPSF-3.0\n" if ( /^%!PS-Adobe-/ )\'';
    $command .= " > $image" if $image;
} else {
    # the normal path
    $command .= " -o$image" if $image;
    $command .= " -T$type $dotfile";
}

$result='';
if ( $image ) {
    $result = `$command`;
} else {
    system($command);
}

my $rc = $?;
if ( ($rc & 127) > 0 ) {
    print STDERR $result if $image;
    die "ERROR: Died on signal", ($rc & 127), "\n";
} elsif ( ($rc >> 8) > 0 ) { 
    print STDERR $result if $image;
    die "ERROR: Unsuccessful execution: ", $rc >> 8, "\n";
} else {
    print STDERR "Successful graphics generation\n";
    exit 0;
}
