#!/bin/sh

HADOOP="/Users/endou/tool/cdh/hadoop-2.0.0-cdh4.4.0/bin/hadoop"

${HADOOP} jar work/MapReduceSampleCdh4.jar:lib/* com.example.hadoop.MapReduceSampleCdh4 /user/endo/sample/input /user/endo/sample/output 
