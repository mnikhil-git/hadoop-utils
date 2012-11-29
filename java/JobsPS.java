import java.io.IOException;

import java.text.DateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.commons.cli.*;

/*
Author : Nikhil Mulley
        mnikhil<at>gmail.com

Yet Another Job Lister for Hadoop.

usage: Hadoop MapReduce Jobs lister.  Available options as follow:
    --d         Run in Verbose/Debug mode (optional)
    --help      when specified, will override and print this help message
    --k         Kill the listed jobs running for more than specified hours
                (optional)
    --l <arg>   List jobs long running for more than specified hours
    --q <arg>   List Jobs in Hadoop MapRed QueueName (optional)
    --u <arg>   List Jobs pertaining to User (optional)

*/

public class JobsPS {


    SimpleGNUCommandLine cli;
    JobConf jcfg = new JobConf();
    JobClient jc;
    String infousage = "Hadoop MapReduce Jobs lister";

    public JobsPS (String[] fargs) throws Exception {
   
    cli = new SimpleGNUCommandLine(fargs, infousage);
    // option, description, hasValue?, isrequired?
    cli.addOption("l", "List jobs long running for more than specified hours ", true, false);
    cli.addOption("q", "List Jobs in Hadoop MapRed QueueName (optional)", true, false);
    cli.addOption("u", "List Jobs pertaining to User (optional)", true, false);
    cli.addOption("k", "Kill the listed jobs running for more than specified hours (optional)", false, false);
    cli.addOption("d", "Run in Verbose/Debug mode (optional)", false, false);


    jc = new JobClient(jcfg);

    }
    
    public void KillJobs(String[] jobidlist) throws Exception {
        for(String jid : jobidlist) {
            System.out.println("Killing jobID : " + jid );
          try {
            jc.getJob(jid).killJob(); 
          } catch (Exception exp) {
            System.err.println("Exception: " + exp.getMessage());
	    exp.printStackTrace();
          }
        }
    }

    public String[] GetJobsinQueue(String queuename) throws Exception {

        if( this.cli.hasOption("d")) System.out.println("Getting Jobs for Queue: " + queuename);
	if (this.cli.hasOption("d") && this.cli.hasOption("u")) System.out.println("Matching user with : " + this.cli.getString("u"));
        long epochInMillis = 0;
        if (this.cli.hasOption("l")) epochInMillis = this.cli.getInteger("l") * 3600000;
	long timeInMillis = System.currentTimeMillis();
	ArrayList<String> jobidlist =new ArrayList<String>();

        try {
          JobStatus[] jsQJobs = jc.getJobsFromQueue(queuename);
          for (JobStatus jobstatus : jsQJobs) {
            String jid = jobstatus.getJobID().toString();
            if (this.cli.hasOption("u")) {
                if  ( (this.cli.getString("u").equals(jobstatus.getUsername()) ) ) {
                  if (this.cli.hasOption("l")) {
                    if (timeInMillis - jobstatus.getStartTime() > epochInMillis) { 
                      System.out.println(" " + jobstatus.getJobID() + " : " + jobstatus.getUsername() + " : " + jc.getJob(jid).getJobName());
                      jobidlist.add(jobstatus.getJobID().toString());
                    }
                  } else {
                    System.out.println(" " + jobstatus.getJobID() + " : " + jobstatus.getUsername() + " : " + jc.getJob(jid).getJobName());
		    jobidlist.add(jobstatus.getJobID().toString());
                  }
                }
            } else {
                if (this.cli.hasOption("l")) {
                  if (timeInMillis - jobstatus.getStartTime() > epochInMillis) { 
                    System.out.println(" " + jobstatus.getJobID() + " : " + jobstatus.getUsername() + " : " + jc.getJob(jid).getJobName());
                    jobidlist.add(jobstatus.getJobID().toString());
                  }
                } else {
                  System.out.println(" " + jobstatus.getJobID() + " : " + jobstatus.getUsername() + " : " + jc.getJob(jid).getJobName());
                  jobidlist.add(jobstatus.getJobID().toString());
                }
                //System.out.println("  " + jobstatus.getJobID());
            }
          }
        } catch (Exception exp) {
          System.err.println("Exception: " + exp.getMessage());
          exp.printStackTrace();
        } 

        return jobidlist.toArray(new String[jobidlist.size()]);
    } 

    public String[] GetMRQueues() throws Exception {
	    ArrayList<String> qnames =new ArrayList<String>();
	    try {
            JobQueueInfo[] jobqs = jc.getQueues();
	    if( this.cli.hasOption("d")) System.out.println("List of Queues on " + jcfg.get("mapred.job.tracker"));
            for (JobQueueInfo jobq : jobqs) {
                qnames.add(jobq.getQueueName());
                if( this.cli.hasOption("d")) System.out.println("-- " + jobq.getQueueName());
            } } catch (Exception exp) {
            System.err.println("Exception: " + exp.getMessage());
            exp.printStackTrace();
            }
            return qnames.toArray(new String[qnames.size()]);
    }

    public static void main(String[] args) throws Exception { 


    JobsPS jobsps = new JobsPS(args);
   
    if( jobsps.cli.hasOption("d")) System.out.println("In Main class ");    
    if( jobsps.cli.hasOption("q")) {
	if( jobsps.cli.hasOption("d")) System.out.println("Selecting Queue " + jobsps.cli.getString("q"));
        String[] jidList = jobsps.GetJobsinQueue(jobsps.cli.getString("q"));
        if(jobsps.cli.hasOption("k")) {
            jobsps.KillJobs(jidList);
        } 
    } else {
        String qnames[] = jobsps.GetMRQueues();
        for (int a = 0; a < qnames.length; a++) {
          if(jobsps.cli.hasOption("k")) {
            if( jobsps.cli.hasOption("d")) System.out.println("In Kill Mode for jobs in Queue : " + qnames[a]);
            jobsps.KillJobs(jobsps.GetJobsinQueue(qnames[a]));
          } else {
	    jobsps.GetJobsinQueue(qnames[a]);
          }
        } // for
    }

    } // end of main
} 
