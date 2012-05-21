#!/bin/bash
#
# $Author:$
# $Id:$
# 
# Fix the under replicated blocks by setting the replication factor for all of the 
# under replicated blocks
#
# Nikhil Mulley
# 
HADOOP_CMD="/usr/bin/hadoop"
HADOOP_REP_FACTOR=3
for hdfsfile in $($HADOOP_CMD fsck / | grep 'Under replicated' | awk -F':' '{print $1}')
do 
  echo "Fixing $hdfsfile :" ;  
  ${HADOOP_CMD} fs -setrep ${HADOOP_REP_FACTOR} $hdfsfile; 
done
