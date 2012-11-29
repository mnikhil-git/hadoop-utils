#!/bin/bash
# Author: Nikhil Mulley
# 
# 20121228
export HADOOP_CLASSPATH=/usr/lib/hadoop/hadoop-core-0.20.2-cdh3u3.jar:/usr/lib/hadoop/hadoop-tools-0.20.2-cdh3u3.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar:/usr/lib/hadoop-0.20/lib/log4j-1.2.15.jar:/usr/lib/hadoop-0.20/lib/commons-logging-1.0.4.jar:../java/.
export CLASSPATH=/usr/lib/hadoop/hadoop-core-0.20.2-cdh3u3.jar:/usr/lib/hadoop/hadoop-tools-0.20.2-cdh3u3.jar:/usr/lib/hadoop-0.20/lib/commons-cli-1.2.jar:/usr/lib/hadoop-0.20/lib/log4j-1.2.15.jar:/usr/lib/hadoop-0.20/lib/commons-logging-1.0.4.jar:../java/.
export HADOOP_HOME=/usr/lib/hadoop

JCLASSNAME="JobsPS"

/usr/bin/hadoop ${JCLASSNAME} "$@"
