/*
* Simple code to extract snapshots from Elasticsearch
*/

SET default_parallel $REDUCERS;
REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();

snapshot = LOAD '$INDEX/$TYPE/_search?q=*:*' USING ESStorage AS (user:map[], timestamp:long, id:long);
STORE snapshot INTO '$OUTPUTDIR' USING BinStorage();
