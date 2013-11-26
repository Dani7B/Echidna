/*
* Simple code to export and count the number of snapshots stored with even or odd id
*/

REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
snap = LOAD 'profiles/snapshot/_search?q=*:*' USING org.elasticsearch.hadoop.pig.ESStorage() AS (user:map[], timestamp:long, id:long);
grouped = GROUP snap BY (id%2);
number = FOREACH grouped GENERATE group, COUNT(snap.timestamp);
STORE number INTO '$OUTPUTDIR';
