/*
* Simple code to extract tweets from Elasticsearch and persist them to HDFS
*/

SET default_parallel $REDUCERS;
REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();

tweets = LOAD '$INDEX/$TYPE/_search?q=tweet.createdAt:[$FROM+TO+$TO]' USING ESStorage AS (id:map[],tweet:map[],monitoringActivityId:chararray);
STORE tweets INTO '$TWEETS' USING BinStorage();