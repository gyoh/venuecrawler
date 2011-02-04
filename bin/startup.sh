#!/bin/sh

NAME=venuecrawler
VERSION=0.1.0
JAVA=/usr/bin/java
JAVA_JAR=$NAME-$VERSION.jar
JAVA_OPTS="-Xms32m -Xmx128m -Dsun.rmi.dgc.client.gcInterval=3600000 -Dsun.rmi.dgc.server.gcInterval=3600000 -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=64m -Xverify:none"
PIDFILE=$NAME.pid

if [ -f $PIDFILE ]; then
  PID=`cat $PIDFILE`
  if (ps -p $PID -o pid > /dev/null); then
    echo "Crawler is already running with pid $PID"
    exit
  fi
fi

$JAVA $JAVA_OPTS -jar $JAVA_JAR & PID=$!
echo $PID > $PIDFILE
