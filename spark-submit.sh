#!/bin/bash

MASTER=spark://localhost:7077
SPARK=/set/spark/dir
exec $SPARK/bin/spark-submit --packages org.rogach:scallop_2.11:latest.integration,graphframes:graphframes:0.7.0-spark2.4-s_2.11 --master $MASTER --class dbpart.spark.Hypercut target/scala-2.11/dbpart_2.11-0.1-SNAPSHOT.jar "$*"

