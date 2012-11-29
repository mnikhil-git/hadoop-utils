#
# $Id: oozie_prune_job_history.pl 13647 2012-10-03 10:23:38Z nikhil.mulley $
# 
# Description:  prune the old records in oozie tables
#                - this will make explicit retention
#                - this will make oozie NOT sluggish.
#
# Nikhil Mulley, InMobi
#
use strict;
use warnings;
use DBI;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use DateTime;
use Data::Dumper qw (Dumper);

# define globals
use vars qw ($oozie_db_host $oozie_db $oozie_db_user $oozie_db_pass 
             $tbl_oozie_coord_jobs $tbl_oozie_coord_actions $tbl_oozie_wf_jobs 
             $tbl_oozie_wf_actions $dbhandle $opt_dryrun $opt_help $USAGE
             $wf_jobs_retention_days $wf_actions_retention_days
             $coord_last_actions_retention $PROG);

# main
&initialization;
&parse_opts;
print "INFO| Running in DRY RUN MODE \n" if $opt_dryrun;
&db_handle;

# prune COORD_ACTIONS table
&prune_oozie_coord_actions;
# prune WF_JOBS 
&prune_oozie_wf_jobs;
# prune WF_ACTIONS
&prune_oozie_wf_actions;


# END block
END {
   # Do some clean up code; disconnect dbhandle
   &disconnect_db; # $dbhandle->disconnect()
   # etc...
}

# define variables
sub initialization {

    umask(002);
    $ENV{PATH} = "/usr/local/bin:/usr/local/etc:/usr/bin:/bin";
    $PROG = basename($0);
    #$PROGLOG = "/var/tmp/$PROG.log";

    # database uri declarations
    $oozie_db_host = "localhost";
    $oozie_db_user = "oozieadmin";
    $oozie_db_pass = "oozieadmin";
    $oozie_db      = "ooziedb2";

    # oozie table declarations
    $tbl_oozie_coord_jobs    = 'COORD_JOBS';
    $tbl_oozie_coord_actions = 'COORD_ACTIONS';
    $tbl_oozie_wf_actions    = 'WF_ACTIONS';
    $tbl_oozie_wf_jobs       = 'WF_JOBS';

    # retention 
    $coord_last_actions_retention = 2500;   
    $wf_jobs_retention_days       = 15; # number of days worth WF_JOBS to keep data (i.e., do not prune)
    $wf_actions_retention_days    = 15; # number of days worth WF_ACTIONS to keep data (i.e., do not prune)

    $opt_dryrun = 0;

    $USAGE = <<EOT
$PROG:   Script to delete the old records from Oozie database.
         Delete the old records from COORD_ACTIONS, WF_JOBS & WF_ACTIONS tables.
--dryrun    Option to run the script in DRYRUN mode. 
            Only prints what happens when script is run, but no DB changes are executed.

--help|-h   prints this cruft.
EOT
;

}

# parsing options
sub parse_opts {
   
    GetOptions(
       'dryrun' => \$opt_dryrun,
       'h|help' => sub {
                    print $USAGE;
                    exit 1;
                  },
       ) or die "ERROR| Please check options provided! \n";

} 
#  connect to oozie database of the respective grid
#
sub db_handle {

    my $dsn = "DBI:mysql:database=$oozie_db;host=$oozie_db_host";

    $dbhandle = DBI->connect($dsn,
                            $oozie_db_user,
                            $oozie_db_pass, { RaiseError => 1, AutoCommit => 0}) or die $DBI::errstr;

    $dbhandle->{InactiveDestroy} = 1;
    $dbhandle->{mysql_auto_reconnect} = 1;


}

#
# get the list of coord_jobs which have higher actions than retention number
# returns a hash reference to coord_job_id as keys ..
# .. to hashes holding coord_job_id, coord appname and the last action number materialized in oozie.
#
sub get_coord_jobs_id {

   my ($db_jobid_qry, $result_ref) = ();
   my ($coord_last_actions_safe_lookup) = int(2 * $coord_last_actions_retention);
   
   print "INFO| Getting the list of Oozie Coordinator Apps from $tbl_oozie_coord_jobs and their last_action history \n";

   $db_jobid_qry  = "SELECT id, app_name, last_action_number from $tbl_oozie_coord_jobs where status = 'RUNNING' ";
   $db_jobid_qry .= " and last_action_number > $coord_last_actions_safe_lookup ";

   $result_ref = $dbhandle->selectall_hashref($db_jobid_qry, 'id');

   if ($result_ref) {
     # return only the coord jobs which have last_action_number greater 2 X $coord_last_actions_retention
     # to be on a safer side and to keep enough required old last actions in COORD_ACTIONS.
     print "INFO| Found ", scalar(keys %$result_ref), 
         " running coordinator apps from $tbl_oozie_coord_jobs ", 
         " whose last_action_number is > $coord_last_actions_safe_lookup \n";
   } else {
     print "WARN| Found NO RUNNING oozie coordinator app in $tbl_oozie_coord_jobs \n";
   }
   return $result_ref;
}

#
# prune oozie WF_JOBS and keep only last 15 days worth of data/records
#
sub prune_oozie_wf_jobs {

    my ($db_del_qry) = ();
   
    print "INFO| In routine prune_oozie_wf_jobs to prune TABLE $tbl_oozie_wf_jobs \n";
    my ($date_string) = DateTime->now->subtract(days => $wf_jobs_retention_days)->ymd;
    $db_del_qry = "DELETE FROM $tbl_oozie_wf_jobs WHERE end_time < '$date_string' ";

    my $d_sth = &myexecute($dbhandle, $db_del_qry);
    if ($d_sth) {
       if ($d_sth->errstr) {
         print "ERROR| Unable to delete the records from TABLE $tbl_oozie_wf_jobs : ", $d_sth->errstr, "\n";
       } else {
         my $d_rows_deleted_wf_jobs = $d_sth->rows;
	 sleep 1;
	 $d_sth->finish;
	 print "INFO| Succesfully deleted $d_rows_deleted_wf_jobs <rows> older than $wf_jobs_retention_days days from TABLE $tbl_oozie_wf_jobs \n";
       } 
    }

}


#
# prune oozie WF_ACTIONS and keep only last 15 days worth of data/records
#
sub prune_oozie_wf_actions {

    my ($db_del_qry) = ();
   
    print "INFO| In routine to prune_oozie_wf_actions to prune TABLE $tbl_oozie_wf_actions \n";
    my ($date_string) = DateTime->now->subtract(days => $wf_actions_retention_days)->ymd;

    $db_del_qry = "DELETE FROM $tbl_oozie_wf_actions WHERE end_time < '$date_string' ";

    my $d_sth = &myexecute($dbhandle, $db_del_qry);
    if ($d_sth) {
       if ($d_sth->errstr) {
         print "ERROR| Unable to delete the records from TABLE $tbl_oozie_wf_actions : ", $d_sth->errstr, "\n";
       } else {
         my $d_rows_deleted_wf_actions = $d_sth->rows;
         sleep 1;
         $d_sth->finish;
         print "INFO| Succesfully deleted $d_rows_deleted_wf_actions <rows> older than $wf_actions_retention_days days from TABLE $tbl_oozie_wf_actions \n";
       }
    }

}


#
#
# prune oozie COORD_JOBS based on last_action_number and keep enough records in retention defined by $coord_last_actions_retention
#
sub prune_oozie_coord_actions {
 
   my ($db_jobid_qry, $result_ref) = ();
   my ($db_del_qry) = ();

   print "INFO| In routine to prune TABLE $tbl_oozie_coord_actions \n";
   my $coord_jobs_info = &get_coord_jobs_id;

   if ($coord_jobs_info) {
    foreach my $coord_job_id ( keys %$coord_jobs_info ) {
      # proceed to delete the old actions based on last_action_item
      print "INFO| Deleting old actions from $tbl_oozie_coord_actions for ". 
              " $coord_jobs_info->{$coord_job_id}->{app_name} : $coord_job_id \n";
      my $last_action = 
              int($coord_jobs_info->{$coord_job_id}->{last_action_number} - $coord_last_actions_retention);
      $db_del_qry  = " DELETE FROM $tbl_oozie_coord_actions WHERE  ";
      $db_del_qry .= " action_number < $last_action ";
      $db_del_qry .= " AND job_id = '$coord_job_id' ";
      print "INFO| Executing :  $db_del_qry \n" if !$opt_dryrun;
      my $d_sth = &myexecute($dbhandle, $db_del_qry);

      if ($d_sth) {
          if ($d_sth->errstr) { 
              print "ERROR| Unable to delete the records for $coord_job_id from $tbl_oozie_coord_actions : ", $d_sth->errstr, "\n";
          } else {
          my $d_rows_deleted_coord_jobs = $d_sth->rows;
          sleep 1;
          $d_sth->finish;
          print "INFO| Succesfully deleted $d_rows_deleted_coord_jobs <rows> for $coord_jobs_info->{$coord_job_id}->{app_name} : $coord_job_id from TABLE $tbl_oozie_coord_actions \n";
	  }
      }
    }
   } else {
       print "WARN| No Coordinator JOBS are found to process further! \n";
   }

}

#
# execute db query
#
sub myexecute($$) {
    my $handle = shift;
    my $command = shift;

    my $sth = $handle->prepare($command) or die "Couldn't prepare " .
        "statement \"$command\": " . $handle->errstr;

    print "INFO: Executing \"$command\"\n";

    if (!$opt_dryrun) {	    
      $sth->execute() or die("Couldn't execute the statement: $command | " . "$sth->errstr");
    }
    return $sth;
}

# disconnect db
sub disconnect_db {
   if ($dbhandle) {
     $dbhandle->disconnect();
   }
}
