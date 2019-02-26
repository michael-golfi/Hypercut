Hypercut is a tool for working with short read NGS data.

Copyright (c) Johan Nyström-Persson 2018-2019.
This is currently at a rough draft stage, and may not even work properly. There are no guarantees. 
For any comments or questions, please contact me at jtnystrom@gmail.com.

Licensed under GPL v3 (see LICENSE.txt). 
Includes parts of the Friedrich framework (GPL license), which is Copyright (c) 
Johan Nyström-Persson and Gabriel Keeble-Gagnere 2010-2015.


To compile:
Install Scala 2.12, SBT, and Kyoto cabinet (native libraries, i.e. libkyotocabinet and 
libjkyotocabinet). The latter can be obtained from here: https://fallabs.com/kyotocabinet/

Then run: sbt compile

Package with: sbt one-jar

Edit run.sh to set the appropriate flags in JAVA_OPTS.
The following java options are recommended, for example (more memory helps if available): 
export JAVA_OPTS="-Djava.library.path=/usr/local/lib -Xmx8g -Xms8g"
libkyotocabinet and libjkyotocabinet are expected in the library path.

Then run with: 
./run.sh --help
to see the command line parameters.

This tool has so far been tested on E.coli (accession ERX008638),
which can be assembled into contigs e.g. as follows:

zcat ERR022075_1.qf.fastq.gz | ./run.sh -k 51 -n 5 -d ERR022075_51_5.kch buckets build -i -
zcat ERR022075_2.qf.fastq.gz | ./run.sh -k 51 -n 5 -d ERR022075_51_5.kch buckets build -i -
./run.sh -d ERR022075_51_5.kch -k 51 -n 5 -m 400  assemble -p 25000 -r              

This uses k=51, extracts 5 markers per k-mer, uses partitions of size 25000 macro nodes when 
performing the piecewise assembly, and filters coverage at 400.
This set of parameters is provided here merely as an example, and may not be ideal in practice.

It should be helpful to use a different tool to quality filter/preprocess the reads before loading data into
Hypercut. FastX is one such tool that may be used: http://hannonlab.cshl.edu/fastx_toolkit/commandline.html