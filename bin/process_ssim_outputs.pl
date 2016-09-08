#!/usr/bin/perl -w
use strict;

my $nfiles = 3552;
my $nseconds = $nfiles / 4;

my %size;
my %ssim;

for (my $i = 0; $i < $nfiles; $i++) {
    my $f;
    open($f, sprintf("<%08d.txt", $i));

    my $quality;
    while (<$f>) {
        if (/^QUALITY:(\d+)$/) {
            $quality = $1;
        } elsif (/^Total (\d+)$/) {
            $size{$quality} += $1;
        } elsif (/^Total: ([0-9.]+) /) {
            my $db = 0 - $1;
            my $val = 10 ** ($db / 10);
            $ssim{$quality} += $val;
        }
    }
}

foreach (sort {$a<=>$b} keys %size) {
    my $quality = $_;
    my $bps = 8 * $size{$quality} / $nseconds;
    my $ssimpct = $ssim{$quality} / $nfiles;
    my $ssimdb = -10 * log($ssimpct) / log(10);
    print "q=$_ bps=$bps ssim%=$ssimpct ssimdB=$ssimdb\n";
}
