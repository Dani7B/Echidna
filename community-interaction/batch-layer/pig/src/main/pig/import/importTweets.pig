/*
* Simple code to extract tweets from Elasticsearch
*/

REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;

/** add the mentioner user too*/
ts = LOAD 'test/tweet/_search?q=*:*' USING org.elasticsearch.hadoop.pig.ESStorage() AS (id:map[],tweet:map[],monitoringActivityId:chararray);
ext = FOREACH ts GENERATE tweet#'createdAt' AS timestamp:long, tweet#'entities' AS entities:map[];
/* ext1 = FOREACH ext GENERATE timestamp, entities#'userMentions' AS mentions:tuple(map[]), user#'id' AS mentioner:long;
* ext2 = FOREACH ext1 GENERATE timestamp, FLATTEN(mentions) AS mention:map[];
* ext3 = FOREACH ext2 {
*		c = mention#'id';
*		GENERATE timestamp, c;
*		};*/
/* ext3 = FOREACH ext2 GENERATE timestamp, mention#'id' AS mentioned:long;
* STORE ext3 INTO '$OUTPUTDIR'; */
STORE ext INTO '$OUTPUTDIR';