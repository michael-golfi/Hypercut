#!/bin/bash

export JAVA_OPTS="-Djava.library.path=/usr/local/lib -XX:+UseParallelGC -Xmx8g -Xms8g"
exec java $JAVA_OPTS -jar target/scala-2.12/dbpart_2.12-0.1-SNAPSHOT-one-jar.jar $*