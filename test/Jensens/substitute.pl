#!/usr/bin/env perl
# $Id$
# substitute @VARIABLE@ with known values
# arguments: N x ( VARIABLE=value | filename | @filelist )

# pre-set some variables
$dbase{HOME} = $ENV{HOME};

# feed assignments, either filenames, filelists or definitions
foreach $arg ( @ARGV ) {
    if ( substr($arg,0,1) eq '@' ) {
	$arg = substr($arg,1);
	#warn "-- filelist $arg\n";
	if ( open( IN, "<$arg") ) {
	    while ( <IN> ) {
		s/\r?\n$//;
		next unless length;
		push @fn, $_;
	    }
	    close(IN);
	} else {
	    warn "unable to read $arg: $!\n";
	}
    } elsif ( ($pos=index($arg,'=')) != -1 ) {
	#warn "-- keyvalue $arg\n";
	$key = substr($arg,0,$pos);
	$dbase{$key} = substr($arg,$pos+1);
    } else {
	#warn "-- filename $arg\n";
	push @fn, $arg;
    }
}

# change files
foreach $fn ( @fn ) {
    my ($infn,$outfn);
    if ( substr( $fn, -3 ) eq '.in' ) {
	# remove suffix
	$infn = $fn;
	$outfn = substr( $fn, 0, -3 );
	# be noisy for .in files
	print "$infn -> $outfn\n";
    } else {
	# make backup copy
	unless ( rename( $fn, "$fn.bak" ) ) {
	    warn "unable to rename $fn to $fn.bak: $!\n";
	    next;
	}
	$infn = "$fn.bak";
	$outfn = $fn;
    }

    # modify files
    if ( open( IN, "<$infn" ) ) {
	if ( open( OUT, ">$outfn" ) ) {
	    while ( <IN> ) {
		foreach $key ( keys %dbase ) {
		    s/\@$key\@/$dbase{$key}/ge;
		}
		print OUT $_;
	    }
	    close(OUT);
	    # preserve original mode
	    chmod( (stat(IN))[2] & 0777, $fn );
	} else {
	    warn "unable to write to $outfn: $!\n";
	}
	close(IN);
    } else {
	warn "unable to read from $infn: $!\n";
    }
}
