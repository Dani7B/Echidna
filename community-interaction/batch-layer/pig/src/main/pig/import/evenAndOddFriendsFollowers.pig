/*
* Simple code to export and count the sum of friends and followers
* of the snapshots stored with even or odd id
*/

SET default_parallel $REDUCERS;
REGISTER /home/daniele/.m2/repository/org/elasticsearch/elasticsearch-hadoop/1.3.0.M1/elasticsearch-hadoop-1.3.0.M1.jar;
snap = LOAD '$INDEX/$TYPE/_search?q=*:*' USING org.elasticsearch.hadoop.pig.ESStorage() AS (user:map[], timestamp:long, id:long);
ff = FOREACH snap GENERATE id, user#'friendsCount' AS friends:long, user#'followersCount' AS followers:long;
grouped = GROUP ff BY (id%2);
number = FOREACH grouped GENERATE group, SUM(ff.friends), SUM(ff.followers);
STORE number INTO '$OUTPUTDIR';
