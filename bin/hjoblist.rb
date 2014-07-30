#!/usr/bin/env ruby
#
# Yet Another Hadoop Job(s) Listing script.
#
require 'rubygems'
require 'trollop'
require 'houdah'
require 'time'
require 'html/table'
require 'net/smtp'
include HTML

#
# command line options
$options_used = ARGV.join(" ")
$cmd_options = Trollop::options do
  banner  'Yet Another Hadoop Job Listing program'
  opt :jobtracker, "Specify the jobtracker hostname", :type => :string, :short => 'j'
  opt :mailto, "Specify the mail recipient for output over email. By default stdout", :type => :string, :short => 'm'
  opt :user, "List of jobs pertaining to specified user", :type => :string, :short => 'u'
  opt :queue, "List of jobs in the specified queue", :type => :string, :short => 'q'
  opt :long_hours_threshold, "List jobs running for more than specified hours", :type => :int, :short => 'l'
  opt :kill_jobs, "(TODO) Kill the jobs found in the list criteria", :short => 'k'
  opt :to_json, "Output in JSON format. Stdout only."
  opt :verbose, "Run in verbose mode", :short => 'v'
end

class Time
  def to_milliseconds
    (self.to_i * 1000.0).to_i
  end
end

def time_diff_milli(start, finish)
  (start - finish)
end

def time_mill_to_human(millis)
  Time.at(millis/1000).strftime("%m/%d/%Y %H:%M")
end

# get the name of the currently logged in user
def get_current_user
  require 'etc'
  return Etc.getlogin
end

# get the list of queues
def get_queues
  puts "INFO| Getting list of queues in cluster" if $cmd_options[:verbose]
  running_queues = Array.new
  begin
    $jobtracker_client.queues.queues.each { |q|
      running_queues << q.queueName
    }
  rescue => ex
    $stderr.puts "ERROR| Unable to get the list of queues"
    $stderr.puts "#{ex.class}: #{ex.message}"
    exit 1
  end
  return running_queues
end

# get the list of jobs in a queue
def get_jobs_in_queue(jqueue)
  if (defined?(jqueue) && (jqueue != '')) then
    $jobs_list.select { |j, jd| jd['queuename'] == jqueue }.each { |jid, jinfo|
      if $cmd_options[:user_given]
        if $cmd_options[:user] == jinfo['user']
          if (jinfo['jobrunning_mins'].to_i > $lthresh_mins.to_i) then
            $found_jobs[jid] = jinfo
          end
        end
      else # no user specified, get all jobs in the queue
        # are long running jobs need to be filtered
        if (jinfo['jobrunning_mins'].to_i > $lthresh_mins.to_i) then
          $found_jobs[jid] = jinfo
        end
      end
    }
  else
    $stderr.puts "ERROR| queue is not given."
    exit 1
  end
end

# get the list of running jobs
def get_running_jobs
  running_jobs = $jobtracker_client.jobs(:running)

  running_jobs.each { |j|
    j_job_id = j.jobID.asString
    $jobs_list[j_job_id]["user"] = j.profile.user
    $jobs_list[j_job_id]["queuename"] = j.profile.queueName
    $jobs_list[j_job_id]["priority"] = $jobs_priority[j.status.priority]
    $jobs_list[j_job_id]["jobrunning_mins"] = (time_diff_milli(Time.now.to_milliseconds, j.status.startTime)) / 1000 / 60
    $jobs_list[j_job_id]["elapsedhour"] = $jobs_list[j_job_id]["jobrunning_mins"] / 60
  }

end

# get the jobtracker for the local cluster
# by default, jobtracker is jobtracker.mydomain.net
def get_jobtracker_host
  require 'xmlsimple'
  mapred_site_xml = '/etc/hadoop/conf/mapred-site.xml'
  jt_host = "jobtracker.mydomain.net"

  begin
    xml_data = XmlSimple.xml_in(mapred_site_xml)
    jt_host = xml_data['property'].select { |k,v|
      k['name'].first.strip == 'mapred.job.tracker'
    }.first['value'].first.split(/:/).first
  rescue => ex
    $stderr.puts "ERROR| Unable to parse the #{mapred_site_xml} file"
  end
    return jt_host
end

# set the globals
def set_globals

  jobtracker_host = ''
  if $cmd_options[:jobtracker_given]
    jobtracker_host = $cmd_options[:jobtracker]
  else
    jobtracker_host = get_jobtracker_host
  end

  $jobtracker_web_url = "http://#{jobtracker_host}:50030/jobdetails.jsp?jobid="
  $lthresh_mins = $cmd_options[:long_hours_threshold] ? $cmd_options[:long_hours_threshold] * 60 : 0

  # initialize the jobtracker client instance with effective user
  $jobtracker_client = Houdah::Client.new(jobtracker_host, port=9290, user=get_current_user)

  # process the user option
  if $cmd_options[:user_given]
    $huser = $cmd_options[:user]
  else
    $huser = get_current_user
  end

  # define the job priority tags
  $jobs_priority =  {
    0 => "VERY_HIGH",
    1 => "HIGH",
    2 => "NORMAL",
    3 => "LOW",
    4 => "VERY_LOW"
  }

  # hash for jobs info
  $jobs_list = Hash.new { |h, k| h[k] = { } }

  # jobs found in the list based on list criteria
  $found_jobs = Hash.new { |h, k| h[k] = { } }

end

# report output over mail in html
def send_report
  msg_from = get_current_user + '@myemail.com'
  msg_to = $cmd_options[:mailto]
  msg_subject = "Hadoop Jobs found for : " + $options_used
  msg_body = <<MESSAGE_END
From:#{msg_from}
To:#{msg_to}
MIME-Version: 1.0
Content-type: text/html
Content-Disposition: inline
Subject: #{msg_subject}
MESSAGE_END

  jobs_table_header = ['Job ID', ' Priority ', ' User ', ' Queue ', 'Running Hours']
  jobs_table_html = HTML::Table.new do
    border   1
    bgcolor 'white'
  end

  jobs_table_html.push Table::Row.new { |r|
    r.content = jobs_table_header
  }

  $found_jobs.each { |jid, jdata|
    jobs_table_html.push Table::Row.new { |r|
      jid_url = "<a href=" + $jobtracker_web_url + jid + '>' + jid + '</a>'
      r.content = [ jid_url, jdata['priority'], jdata['user'], jdata['queuename'], jdata['elapsedhour'] ]
    }
  }

  msg_body += jobs_table_html.html

  begin
    smtp = Net::SMTP.start('localhost', 25)
    smtp.send_message msg_body, msg_from, msg_to
    smtp.finish
  rescue => ex
    $stderr.puts "ERROR| Unable to send mail"
    $stderr.puts "#{ex.class}: #{ex.message}"
    exit 1
  end

end

# main
def list_jobs
  set_globals
  get_running_jobs

  if $cmd_options[:queue]
    get_jobs_in_queue($cmd_options[:queue])
  else
    # get the list of jobs in all the queues
    get_queues.each { |q|
      get_jobs_in_queue(q)
    }
  end

  if $cmd_options[:to_json]
    require 'json'
    puts $found_jobs.to_json
  elsif $cmd_options[:mailto]
    send_report
  else
    $found_jobs.each { |jid, jinfo|
      puts jid + "|" + jinfo['user'] + "|" + jinfo['queuename'] + "|" + jinfo['priority']
    }
  end

end

list_jobs  
