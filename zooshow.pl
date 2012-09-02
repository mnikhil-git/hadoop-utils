#!/usr/bin/perl

# $Header$
# $Id$
# $Author$
#
# Nikhil Mulley
#
# read the list of zookeeper servers from the configuration file
# get the mode status of them. Useful to know who is current leader of ensemble.

use IO::Socket;
use warnings;
use strict ;
use Carp;
use Data::Dumper;

use vars qw ($zoo_cfg $clients_port $zoo_servers $zoo_info $verbose);

&initialization;
&read_zoo_config;
&get_zoo_details;
&show_zoo;

sub initialization {
   
    $zoo_cfg = "/etc/zookeeper/zoo.cfg";
    #$zoo_cfg = "zoo.cfg";
    $verbose = 0; 

}

sub show_zoo {
   
    foreach my $zoo_server ( sort keys %{$zoo_info} ) {
      printf ("%20s | %5s | %-10s|\n", 
                 $zoo_server, 
                 $zoo_info->{$zoo_server}->{'version'}, 
                 $zoo_info->{$zoo_server}->{'mode'}
             );
    }

}

sub get_zoo_details {

   my $stat_cmd = "stat\n";  
   foreach my $zoo_server (&get_zoo_servers) {

     print "INFO| Connecting to $zoo_server:$clients_port \n" if $verbose; 
     my $zoo_conn = IO::Socket::INET->new(
		     PeerAddr => $zoo_server,
		     PeerPort => $clients_port,
		     Proto    => 'tcp',
		     Timeout  => 60,
		     Type     => SOCK_STREAM,
		     ) or croak "Could not connect to $zoo_server:$clients_port: $@";

     print $zoo_conn $stat_cmd;

     while(<$zoo_conn>) {
	     chomp;
             if (my ($attr, $value) = (split/:/, $_)) {
               if ($attr =~ /Zookeeper version/) {
                 ($zoo_info->{$zoo_server}->{'version'} = trim($value)) =~ s/(\d+\.\d+\.\d+)-.*/$1/;
               }  
               if ($attr =~/Mode/) {
                 $zoo_info->{$zoo_server}->{'mode'} = trim($value);
               }
	       # print "DUMP: $attr --> $value \n";#push(@answer, $_);
             }
     }

     close ($zoo_conn);
  }

     #print Dumper($zoo_info);
}

sub read_zoo_config {
    
    if (open(ZOOCFG, "<$zoo_cfg")) {
      while (<ZOOCFG>) {
        my $line = $_;
        chomp $line;
        $clients_port = $1 if ($line =~ /^clientPort=(\d+)/);
	if ($line =~ /^server\.(\d+)=(\w+.*):\d+:\d+/) {
          $zoo_servers->{$1} = $2;
        }
      }
    } else {
      die "ERROR| Failed to open $zoo_cfg : $! \n";
    } # end if

}

# returns list of zoo servers
sub get_zoo_servers {
 
   if ($zoo_servers) {
     return values %{$zoo_servers};
   }

}

# trim whitespace around
sub trim {

  my ($to_return) = (@_);
  $to_return =~ s/^\s+//;
  $to_return =~ s/\s+$//;
  return ($to_return);

}
