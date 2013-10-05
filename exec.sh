#!/bin/sh

HADOOP="/Users/endou/tool/cdh/hadoop-2.0.0-cdh4.4.0/bin/hadoop"
HDFS_ROOT="hdfs://192.168.56.102:8020"

hdfs dfs -rm -r ${HDFS_ROOT}/user/endo/sample/output 
export HADOOP_CLASSPATH=conf

${HADOOP} jar ./work/MapReduceSampleCdh4.jar com.example.hadoop.MapReduceSampleCdh4 ${HDFS_ROOT}/user/endo/sample/input ${HDFS_ROOT}/user/endo/sample/output 
