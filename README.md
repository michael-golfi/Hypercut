## Overview

Hypercut is a tool for working with short read NGS data.

Copyright (c) Johan Nyström-Persson 2018-2019.
This is currently under development, and may not work properly. There are no guarantees. 
For any comments or questions, please contact me at jtnystrom@gmail.com.

Licensed under GPL v3 (see LICENSE.txt). 
The source code includes parts of the Friedrich framework (GPL license), authored by
Johan Nyström-Persson and Gabriel Keeble-Gagnere. It also includes parts of Scalgos by Pathikrit Bowmick.

Hypercut is being developed with Scala 2.11 and Spark 2.4, to match the current Google Cloud dataproc images.
However, if you run your own Spark cluster, using Scala 2.12 should not be a problem.
In addition to a working Spark installation, the sbt build tool is also needed.

## Compiling
The command `sbt package` will produce the necessary jar file.

Copy spark-submit.sh.template to spark-submit.sh and edit the necessary variables in the file.
Alternatively, if you submit to a GCloud cluster, you can use submit-gcloud.sh.template.
For the standalone data analysis tool, copy run.sh.template to run.sh and edit.

## Usage

Hypercut can be used to produce a compacted De Bruijn graph. The supported input format is currently 
fasta files (uncompressed) only. (Compressed .gz and .bz2 files cannot be handled efficiently by Spark).

It should be helpful to use a different tool to quality filter/preprocess the reads before loading data into
Hypercut. FastX is one such tool that may be used: http://hannonlab.cshl.edu/fastx_toolkit/commandline.html

There are two main steps to using the tool: bucket building and graph compaction.
When buckets have been built, they will be stored on disk and can be compacted several times using different parameters
to find the best configuration.

Example bucket build command:

`
./spark-submit.sh  -k 41 -n 3 --space all3 --sample 0.01 buckets -l /data/dir/mydata_out build -i "/data/dir/*.fasta"     
`

This will set k=41 and sample 3 "motifs" per k-mer. 0.01 fraction (1%) of the reads will be used to determine motif frequencies.
The input data will be all the files listed after -i; outputs will be stored in the directory specified by -l.
The --help flag can be used to see additional commands and parameters.

Example graph compaction command, after buckets have been built:

`
 ./spark-submit.sh  -k 41 -n 3  buckets -l /data/dir/mydata_out compact -m 100 --show-stats
`

This would output unitigs whose abundance (solidity) is at least 100. --show-stats is optional and removing it may speed up processing somewhat.
Output sequences are placed in csv files in the directory /data/dir/mydata_out_unitigs (the unitigs suffix is appended automatically).
The path supplied must be identical to what was used in the build command previously.


