#!/usr/bin/perl
#
# $Id: hdfs-quota-df-top.pl 12438 2012-08-27 03:59:28Z nikhil.mulley $
# $Author: nikhil.mulley $
# $HeadURL:$
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
use Parallel::ForkManager;
use Pod::Usage;
use Data::Dumper qw (Dumper);

# globals
use vars qw ($PROG $PROGLOG $HADOOP_CMD @HDFS_NAMESPACE $TOP_LIMIT
             $hdfs_topdir @hdfs_abs_dirs $verbose %hdfs_quota_info $sort_by_col 
             $hadoop_conf %cols $GANGLIA);

# main
&initialization;
&parse_opts;

if ( defined $hdfs_topdir ) {
    $hdfs_topdir ="/$hdfs_topdir" unless $hdfs_topdir =~ /^\//;
} else {
    $hdfs_topdir = "/user";
}

print "INFO | processing quota information for directories under $hdfs_topdir  \n" if $verbose;

&get_hdfs_quota_info($hdfs_topdir);

if ($GANGLIA) {
   print "INFO| Running in ganglia mode \n" if $verbose;
   die "ERROR| Which Hadoop grid is this run for? \n" if !defined ($hadoop_conf);
   &inform_ganglia;
} else {
   &report_hdfs_top_df;
}
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
    $hadoop_conf = `facter hadoop_conf`; chomp $hadoop_conf;
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
        'sortby=s' => \$sort_by_col,
        'l=i' => \$TOP_LIMIT,
        'ganglia' => \$GANGLIA,
        'verbose' => \$verbose,
        'help|h|?' => \$help
    ) or pod2usage(2);

    pod2usage(1) if $help;

}

# read the quota usage information
sub get_hdfs_quota_info {
   
    my ($hdfs_top_namespace) = (@_);
    my $run_cmd = "$HADOOP_CMD  fs -count -q \"$hdfs_top_namespace/*\"";

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
| HDFS Directory            Allocated Quota          Used Space       Used (%)   |
|------------------------|----------------------|-----------------|--------------|
.

   format REPORT_HANDLE_FOOTER =
|------------------------|----------------------|-----------------|--------------|
.

   format REPORT_HANDLE =
| @<<<<<<<<<<<<<<<<<<<<< | @||||||||||||||||||| |  @||||||||||||| |   @<<<<<<<<< |
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

    $cmp_col_key = $cols{$sort_by_col} if ( defined($sort_by_col)  &&  exists($cols{$sort_by_col}) ) ;

    foreach my $hdfs_quota_dir (
                           sort {
                             $hdfs_quota_info{$b}{$cmp_col_key} <=> $hdfs_quota_info{$a}{$cmp_col_key} 
                           } @hdfs_dirs_list ) {
            $directory    = $hdfs_quota_dir;
            $allocation   = trim($hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA_FORMATTED'});
            $hdfs_used    = trim($hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE_FORMATTED'});
            $percent_used = trim($hdfs_quota_info{$hdfs_quota_dir}{'PERCENTAGE_USED_SPACE'});
            write;
            $iterator += 1;
            last if $iterator == $TOP_LIMIT && $TOP_LIMIT != -1;;
      } # end of foreach

    $~ = 'REPORT_HANDLE_FOOTER'; write;
    } # end if

}


# emit metrics to ganglia for graphing
sub inform_ganglia {
   my ($hdfs_directory, $hdfs_allocation, $hdfs_used) = ();
   my ($iterator) = 0;
   my ($metric_step) = 60 * 15; # 15 minutes
   my ($MAX_GMETRIC_PROC) = 5;
   if (%hdfs_quota_info) {

   my @hdfs_dirs_list = keys %hdfs_quota_info;

   if ( @hdfs_abs_dirs && grep { /$hdfs_topdir/ } @hdfs_abs_dirs ) {
     @hdfs_dirs_list = ();
     foreach ( @hdfs_abs_dirs ) {
       chomp;
       push ( @hdfs_dirs_list, $_ ) if ( defined($hdfs_quota_info{$_}) ) ;
     }
   }

   my $gm_pm = new Parallel::ForkManager($MAX_GMETRIC_PROC);

   $gm_pm->run_on_finish(
		   sub { my ($pid, $exit_code, $ident) = @_;
		   print "***** $ident just got finished ".
		   "with PID $pid and exit code: $exit_code\n" if $verbose;
		   }
		   );

   $gm_pm->run_on_start(
		   sub { my ($pid,$ident)=@_;
		   print "***** $ident just started, pid: $pid\n" if $verbose;
		   }
		   );

   foreach my $hdfs_quota_dir ( @hdfs_dirs_list ) {
	   $gm_pm->start($hdfs_quota_dir) and next; # do the fork

           ($hdfs_directory = $hdfs_quota_dir) =~ s/\//_/g; 
           $hdfs_allocation   = trim($hdfs_quota_info{$hdfs_quota_dir}{'SPACE_QUOTA'});
           $hdfs_used    = trim($hdfs_quota_info{$hdfs_quota_dir}{'USED_SPACE'});
           my $alloc_metric_cmd = "gmetric -n \"${hadoop_conf}.quota.allocation.${hdfs_directory}\" -v ${hdfs_allocation} -u Kilobytes -t int32 -d ${metric_step} ";
           my $used_metric_cmd =  "gmetric -n \"${hadoop_conf}.quota.usage.${hdfs_directory}\" -v ${hdfs_used} -u Kilobytes -t int32 -d ${metric_step} ";
           print "INFO| Running $alloc_metric_cmd \n" if $verbose;
           system($alloc_metric_cmd);
           print "INFO| Running $used_metric_cmd \n" if $verbose;
           system($used_metric_cmd);
    
           $gm_pm->finish;
           #$iterator += 1;
           #last if $iterator == $TOP_LIMIT && $TOP_LIMIT != -1;;
           
     } # end of foreach
   $gm_pm->wait_all_children; # have all forked process exited?
   } # end if

}

# trim whitespaces
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

=item B<-ganglia>

Emit ganglia metrics using gmetric for HDFS Quota allocation/usage of directories.

=item B<-sortby>

Use --sortby if needed to sort the output based on the allocation or usage fields.
The default output is with percentage of used space. Accepts only one value.
If needed to sort by allocation, use allocation as value to --sortby option.

  Example : --sortby allocation

If needed to sort by used,

  Example : --sortby used

=back
=head1 DESCRIPTION

B<This program> will read the quota information of the given HDFS directory
and make some useful with the contents thereof.

=cut
