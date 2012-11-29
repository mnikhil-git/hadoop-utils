#!/usr/bin/perl

# $Header$
# $Id$
# $Author$
#
# Nikhil Mulley
#
# read the list of zookeeper servers from the configuration file
# get the mode status of them. Useful to know who is current leader of ensemble.

use warnings;
use strict ;
use Net::ZKMon;

use vars qw ($zoo_cfg $clients_port $zoo_servers $zoo_info $verbose);

&initialization;
&read_zoo_config;
&show_zoo;

sub initialization {
   
    $zoo_cfg = "/etc/zookeeper/zoo.cfg";
    #$zoo_cfg = "zoo.cfg";
    $verbose = 0; 

}

sub show_zoo {

    my $zkmon = new Net::ZKMon;

    foreach my $zoo_server ( &get_zoo_servers ) {
      my $z_server_stat = $zoo_server->stat($zoo_server);
      printf "%20s | %5s | %-10s|\n", 
                 $zoo_server, 
                 $z_server_stat->{'version'}, 
                 $z_server_stat->{'Mode'}, 
    }

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
