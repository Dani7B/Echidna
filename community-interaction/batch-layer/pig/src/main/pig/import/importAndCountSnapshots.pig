/*
* Simple code to extract and count snapshots from Elasticsearch
*/

SET default_parallel $REDUCERS;
REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
snap = LOAD '$INDEX/$TYPE/_search?q=*:*' USING org.elasticsearch.hadoop.pig.ESStorage() AS (user:map[], timestamp:long, id:long);
grouped = GROUP snap BY id;
number = FOREACH grouped GENERATE group, COUNT(snap);
STORE number INTO '$OUTPUTDIR';
