#!/bin/bash

# $Header$
# $Id: emit_hdfs_quota_metrics.sh 12436 2012-08-27 03:48:04Z nikhil.mulley $
# $Author: nikhil.mulley $

# Nikhil Mulley
# 
# Wrapper to emit the HDFS Quota allocation/usage metrics to Ganglia for the enabled directories
GANGLIA_HDFS_QUOTA_EMITTER="perl /opt/hdfs_quota_mon/hdfs-quota-df-top.pl --ganglia --verbose"
TOP_LEVEL_DIRS=("/projects" "/project" "/user" "/data")

for DIR in "${TOP_LEVEL_DIRS[@]}"
do
   echo "Running Quota metrics for ${DIR} "
   ${GANGLIA_HDFS_QUOTA_EMITTER} -d ${DIR}
done

