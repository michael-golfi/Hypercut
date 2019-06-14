#!/bin/bash

SPARK=/set/spark/dir
MASTER=spark://localhost:7077

exec $SPARK/bin/spark-shell --jars target/scala-2.11/dbpart_2.11-0.1-SNAPSHOT.jar --packages org.rogach:scallop_2.11:latest.integration,graphframes:graphframes:0.7.0-spark2.4-s_2.11 --master $MASTER


