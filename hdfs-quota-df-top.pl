#!/usr/bin/perl
#
# $Id: hdfs-quota-df-top.pl 9255 2012-06-08 16:00:41Z nikhil.mulley $
# $Author: nikhil.mulley $
# $HeadURL: https://svn.corp.inmobi.com/inmobi/operations/gridops/tools/scripts/src/opt/gridscripts/spaceQuotaMon/hdfs-quota-df-top.pl $
# 
# Description: report the top quota usage directories in HDFS
#
# Nikhil Mulley, InMobi
#
use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use Number::Bytes::Human qw(format_bytes);
use Pod::Usage;
use Data::Dumper qw (Dumper);

# globals
use vars qw ($PROG $PROGLOG $HADOOP_CMD @HDFS_NAMESPACE $TOP_LIMIT
             $hdfs_topdir @hdfs_abs_dirs $verbose %hdfs_quota_info );

# main
&initialization;
&parse_opts;

if ( defined $hdfs_topdir ) {
    $hdfs_topdir ="/$hdfs_topdir" unless $hdfs_topdir =~ /^\//;
} else {
    $hdfs_topdir = "/user";
}

&get_hdfs_quota_info($hdfs_topdir);
&report_hdfs_top_df;
exit;

# end of main

# setup variables et all
sub initialization {

    umask(002);
    $ENV{PATH} = "/usr/local/bin:/usr/local/etc:/usr/bin:/bin";
    $HADOOP_CMD = "/usr/bin/hadoop";
    $PROG = basename($0);
    $PROGLOG = "/var/tmp/$PROG.log";
    $TOP_LIMIT = 10;
    %hdfs_quota_info = ();

}

# process command line arguments
sub parse_opts {

    my $help = 0;

    GetOptions(
        'd=s' => \$hdfs_topdir,
        'path:s{0,9}' => \@hdfs_abs_dirs,
        'l=i' => \$TOP_LIMIT,
        'verbose' => \$verbose,
        'help|h|?' => \$help
    ) or pod2usage(2);

    pod2usage(1) if $help;

}

# read the quota usage information
sub get_hdfs_quota_info {
   
    my ($hdfs_top_namespace) = (@_);
    my $run_cmd = "$HADOOP_CMD  fs -count -q $hdfs_top_namespace/*";

    my @quota_info_namespace = `$run_cmd`;
    my $exit_status = $? >> 8;

    if (! $exit_status ) {

        foreach ( @quota_info_namespace ) {

          chomp;
          # Format
          # QUOTA REMAINING_QUOTA SPACE_QUOTA REMAINING_SPACE_QUOTA DIRS FILES SIZE HDFS_QUOTA_DIR 
	  my (undef, 
              undef, 
              undef,
              $space_quota, 
              $remaining_space_quota, 
              $dir_cnt, 
              $file_cnt, 
              $cont_size, 
              $hdfs_dir) = split(/\s+/, $_);       

          $hdfs_dir =~ /.*\w+($hdfs_top_namespace\/\w+.*)/;
          my ($hdfs_quota_dir) = $1;
          if ( defined($hdfs_quota_dir) && 
                   $space_quota =~ /[[:digit:]]$/ &&
                   $remaining_space_quota =~ /[[:digit:]]$/ ) { 

            #print "VERBOSE : Collecting quota information for $hdfs_quota_dir \n";
            $hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA'} = $space_quota;
            $hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA_FORMATTED'} = format_bytes($space_quota);
            $hdfs_quota_info{$hdfs_quota_dir}{'REMAINING_SPACE_QUOTA'} = $remaining_space_quota;
            $hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE'} = 
                int(int($space_quota) - int($remaining_space_quota));
            $hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE_FORMATTED'} = 
                format_bytes(int(int($space_quota) - int($remaining_space_quota)));
            $hdfs_quota_info{$hdfs_quota_dir}{'PERCENTAGE_USED_SPACE'} = 
                sprintf "%.2f ", ( ($space_quota - $remaining_space_quota) * 100 ) / $space_quota;
            $hdfs_quota_info{$hdfs_quota_dir}{'HDFS_FILENAME'} = $hdfs_dir;
            $hdfs_quota_info{$hdfs_quota_dir}{'DIR_COUNT'} = $dir_cnt;
            $hdfs_quota_info{$hdfs_quota_dir}{'FILE_COUNT'} = $file_cnt;

	  }

	}

    }
#        print Dumper (%hdfs_quota_info);
    
}

# sort the usage fields in descending order
# report the output
sub report_hdfs_top_df {

   my ($directory, $allocation, $hdfs_used, $percent_used) = ();
   my ($iterator) = 0;
   #print "VERBOSE : $TOP_LIMIT \n";

   format REPORT_HANDLE_HEADER =
HDFS Directory          Allocated Quota      Used Space       Used (%)
----------------------------------------------------------------------
.

   format REPORT_HANDLE =
@<<<<<<<<<<<<<<<<<<<<< @|||||||||||||||||||  @|||||||||||||   @<<<<<<<<<
$directory, $allocation, $hdfs_used, $percent_used
.

    if (%hdfs_quota_info) {

    my @hdfs_dirs_list = keys %hdfs_quota_info;

    $~ = 'REPORT_HANDLE_HEADER'; write;
    $~ = 'REPORT_HANDLE'; 

    if ( @hdfs_abs_dirs && grep { /$hdfs_topdir/ } @hdfs_abs_dirs ) {
      @hdfs_dirs_list = ();
      foreach ( @hdfs_abs_dirs ) {
        chomp;
        push ( @hdfs_dirs_list, $_ ) if ( defined($hdfs_quota_info{$_}) ) ;
      }
    }

    foreach my $hdfs_quota_dir (
                           sort {
                             $hdfs_quota_info{$b}{'PERCENTAGE_USED_SPACE'} <=> $hdfs_quota_info{$a}{'PERCENTAGE_USED_SPACE'} 
                           } @hdfs_dirs_list ) {
            $directory    = $hdfs_quota_dir;
            $allocation   = trim($hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA_FORMATTED'});
            $hdfs_used    = trim($hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE_FORMATTED'});
            $percent_used = trim($hdfs_quota_info{$hdfs_quota_dir}{'PERCENTAGE_USED_SPACE'});
            write;
            $iterator += 1;
            last if $iterator == $TOP_LIMIT;
      } # end of foreach

    } # end if

}


sub trim {

 my ($a) = (@_);
 $a =~ s/^\s+//;
 $a =~ s/\s+$//;
 return($a);

}
__END__

=head1 NAME

$PROG  -Report the top directories usage in HDFS
POD to be updated
=head1 SYNOPSIS

$PROG [options] [-l lines]

Options:

-help brief help message

-l    limit the output to top lines

=head1 OPTIONS

=over 8

=item B<-help>

Print a brief help message and exits.

=item B<-l>

Provide the number to limit the output to top -l lines

=back
=head1 DESCRIPTION

B<This program> will read the quota information of the given HDFS directory
and make some useful with the contents thereof.

=cut
