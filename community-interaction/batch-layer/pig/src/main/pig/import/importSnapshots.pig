/*
* Simple code to extract snapshots from Elasticsearch
*/

REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
snapshot = LOAD 'profiles/snapshot/_search?q=*:*' USING org.elasticsearch.hadoop.pig.ESStorage() AS (user:map[], timestamp:long, id:long);
STORE snapshot INTO '$OUTPUTDIR' USING BinStorage();
