hdfs dfs -rm -r /output &&
hadoop jar CountingIndexer.jar CountingIndexer /input_small /output &&
rm -r output/ &&
hdfs dfs -copyToLocal /output