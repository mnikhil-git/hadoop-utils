#!/usr/bin/perl
#
use Getopt::Long;
use XML::Simple;

# Script to get the current Active Namenode in HDFS cluster

use vars qw($VERSION $hdfs_cmd %opts %conf $ha_client_config $xml_simple);

$VERSION = '0.01';
$hdfs_cmd = "/usr/bin/hdfs";
$ha_client_config = "/etc/hadoop/conf/haclient.xml";

my $usage = "$0 -c <ha-cluster-service> \n";
my $ha_cluster = "";
my $verbose = undef;
$xml_simple = XML::Simple->new;

unless ( -x $hdfs_cmd ) {
  die "ERROR| $hdfs_cmd does not exist \n";
}

unless ( -e $ha_client_config ) {
  die "ERROR| $ha_client_config is missing! \n";
}

GetOptions("c=s" => \$ha_cluster,
           'h' => sub { print $usage; exit 0;},
           'v' => \$verbose ) or
  die $usage;

if ( defined $ha_cluster ) {
  &read_config;
  unless(defined $conf{$ha_cluster} && (exists($conf{$ha_cluster})) ) {
    die "ERROR| cluster argument is not correct. \n";
  }
  my $active_host = &get_active_namenode($ha_cluster);
  print $conf{$ha_cluster}->{$active_host}, "\n";
}

sub read_config {
  my $xml_data = $xml_simple->XMLin($ha_client_config);
  my @dfs_nameservices = (split(/,/, $xml_data->{'property'}->{'dfs.nameservices'}->{'value'}));
  foreach my $dfs_ns (@dfs_nameservices) {
    chomp;
    my $dfs_ns_property = "dfs.ha.namenodes.$dfs_ns";
    my @dfs_nn_hosts = (split(/,/, $xml_data->{'property'}->{$dfs_ns_property}->{'value'}));
    foreach my $dfs_nn_host ( @dfs_nn_hosts ) {
      chomp $dfs_nn_host;
      my $dfs_nn_host_property = "dfs.namenode.rpc-address.${dfs_ns}.${dfs_nn_host}";
      $conf{$dfs_ns}->{$dfs_nn_host} = $xml_data->{'property'}->{$dfs_nn_host_property}->{'value'};
    }
  }
  return;
}


sub get_active_namenode {
  my ($ha_cluster) = @_;
  my @nn_hosts =     map { $_ =~ /ha-nn/ ? $_ : () } keys %{$conf{$ha_cluster}};
  foreach my $nn_host ( sort @nn_hosts ) {
    my $haadmin_cmd = "${hdfs_cmd} haadmin -conf $ha_client_config -ns ${ha_cluster} -getServiceState ${nn_host} 2>/dev/null";
    print "INFO| Executing $haadmin_cmd \n" if $verbose;
    my $haadmin_out;
    if (open(my $ha_status, "-|", "$haadmin_cmd ")) {
      while(<$ha_status>) {
        chomp;
        $haadmin_out = $_;
        if ($haadmin_out =~ "active") {
          return $nn_host;
        }
      }
      close ($ha_status);
    } else {
      print STDERR "ERROR| Unable to execute haadmin command : $@\n";
    }
  }
  return;
}
