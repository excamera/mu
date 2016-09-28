#!/usr/bin/perl -w
use strict;

if (scalar(@ARGV) < 3) {
    die "Usage: $0 <infile> <outpattern> <nframes> ['yesplease']";
}

my $nframes = int($ARGV[2]);
if ($nframes == 0) {
    die "nframes must be nonzero!";
}


open my $infile, "<" . $ARGV[0] or die "Could not open $ARGV[0]: $!";
my @inlines = <$infile>;
close($infile);

# if we're asked to do it in place, delete the input file before continuing
if (scalar(@ARGV) > 3 && $ARGV[3] eq "yesplease") {
    unlink($ARGV[0]);
}

my $header = shift @inlines;
my ($width, $height, $space);
               # YUV4MPEG2 W854      H480      F24:1          Ip A0:0           C420jpeg          XYSCSS=420JPEG
if ($header =~ /^YUV4MPEG2 W([0-9]+) H([0-9]+) F[0-9]+:[0-9]+ I. A[0-9]+:[0-9]+ C([0-9]{3})[a-z]*.*$/) {
    $width = $1;
    $height = $2;
    $space = $3;
    print STDERR "w = $width, h = $height, s = $space\n";
} else {
    die "ERROR invalid header $header";
}

my $readlen = $width * $height;
if ($space eq '420') {
    $readlen *= 3;
    $readlen /= 2;
} elsif ($space eq '422') {
    $readlen *= 2;
} elsif ($space eq '444') {
    $readlen *= 4;
} else {
    die "ERROR invalid colorspace param $space";
}

my $outfile;
for (my $i = 0; ; $i++) {
    $_ = shift @inlines;
    if (length($_) == 0) {
        last;
    }

    # open a new file every $nframes frames
    if ($i % $nframes == 0) {
        close $outfile if defined($outfile);
        open $outfile, sprintf('>%s%06d.y4m', $ARGV[1], $i / $nframes);
        print $outfile $header;
        print STDERR '.';
    }

    my $nextframe;
    if (length($inlines[0]) < $readlen) {
        die "ERROR: tried to read $readlen from file";
    } else {
        $nextframe = substr($inlines[0], 0, $readlen, '');
    }

    print $outfile $_;
    print $outfile $nextframe;
}

close $outfile if defined($outfile);
print STDERR "\n";
