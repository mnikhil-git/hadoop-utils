#!/usr/bin/perl
#
<<<<<<< HEAD
# $Id: hdfs-quota-df-top.pl 9645 2012-06-20 13:34:49Z nikhil.mulley $
# $Author: nikhil.mulley $
# $HeadURL: $
=======
# $Id: hdfs-quota-df-top.pl 9255 2012-06-08 16:00:41Z nikhil.mulley $
# $Author: nikhil.mulley $
# $HeadURL:$
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47
# 
# Description: report the top quota usage directories in HDFS
#
# Nikhil Mulley
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
<<<<<<< HEAD
             $hdfs_topdir @hdfs_abs_dirs $verbose %hdfs_quota_info $sort_by_col %cols);
=======
             $hdfs_topdir @hdfs_abs_dirs $verbose %hdfs_quota_info );
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47

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
    %cols = (
#            'name'       => 'HDFS_DIRECTORY',
             'allocation' => 'SPACE_QUOTA',
             'used'       => 'USED_SPACE',
            );   

}

# process command line arguments
sub parse_opts {

    my $help = 0;

    GetOptions(
        'd=s' => \$hdfs_topdir,
        'path:s{0,9}' => \@hdfs_abs_dirs,
<<<<<<< HEAD
        'sortby=s' => \$sort_by_col,
=======
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47
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
            $hdfs_quota_info{$hdfs_quota_dir}{'HDFS_DIRECTORY'} = $hdfs_quota_dir;
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
   my ($cmp_col_key) = 'PERCENTAGE_USED_SPACE';
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

<<<<<<< HEAD
    $cmp_col_key = $cols{$sort_by_col} if ( defined($sort_by_col)  &&  exists($cols{$sort_by_col}) ) ;

    foreach my $hdfs_quota_dir (
                           sort {
                             $hdfs_quota_info{$b}{$cmp_col_key} <=> $hdfs_quota_info{$a}{$cmp_col_key} 
=======
    foreach my $hdfs_quota_dir (
                           sort {
                             $hdfs_quota_info{$b}{'PERCENTAGE_USED_SPACE'} <=> $hdfs_quota_info{$a}{'PERCENTAGE_USED_SPACE'} 
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47
                           } @hdfs_dirs_list ) {
            $directory    = $hdfs_quota_dir;
            $allocation   = trim($hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA_FORMATTED'});
            $hdfs_used    = trim($hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE_FORMATTED'});
            $percent_used = trim($hdfs_quota_info{$hdfs_quota_dir}{'PERCENTAGE_USED_SPACE'});
            write;
            $iterator += 1;
<<<<<<< HEAD
            last if $iterator == $TOP_LIMIT && $TOP_LIMIT != -1;;
=======
            last if $iterator == $TOP_LIMIT && $TOP_LIMIT != -1;
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47
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

-l    limit the output to top few lines. Default is 10 and with -1 to impose no limits.

=head1 OPTIONS

=over 8

=item B<-help>

Print a brief help message and exits.

=item B<--path>

Provide the hdfs directorie(s) to specifically query the quota of them.

 Example : --path /user/adrel /user/mnikhil

=item B<-d>

Provide the hdfs top level directory name space to query the HDFS quota directories under it.

 Example : -d /user

 Example : -d /projects.

 Multiple options are not supported at this point.

=item B<-l>

Provide the number to limit the output to top -l number of lines.
  Default value is 10 unless the option is specified.
  -1 for imposing no limits on the output.
<<<<<<< HEAD

=item B<-sortby>

Use --sortby if needed to sort the output based on the allocation or usage fields.
The default output is with percentage of used space. Accepts only one value. 
If needed to sort by allocation, use allocation as value to --sortby option.

  Example : --sortby allocation

If needed to sort by used space,

  Example : --sortby used
=======
>>>>>>> 3ab8fe031cc4cf7a80e5d9e9767144945d9bfe47

=back
=head1 DESCRIPTION

B<This program> will read the quota information of the given HDFS directory
and make some useful with the contents thereof.

=cut
