/*
* Simple code to extract follow and mention relationships from Elasticsearch
*/

REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();

mention = LOAD 'mentions/mention/_search?q=timestamp:[$FROM+TO+$TO]' USING ESStorage 
				AS (mentioner:long, mentioned:long, timestamp:long);
STORE mention INTO '$MENTIONS' USING BinStorage();

follows = LOAD 'follows/follow/_search?q=timestamp:[$FROM+TO+$TO]' USING ESStorage 
				AS (follower:long, followed:long, timestamp:long);
STORE follows INTO '$FOLLOWS' USING BinStorage();
