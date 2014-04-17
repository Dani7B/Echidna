/*
* Simple code to load tweets from HDFS and extract mentioner-mentioned information out of it
*/

SET default_parallel $REDUCERS;

tweets = LOAD '$TWEETS/part*' USING BinStorage() AS (id:map[],tweet:map[],monitoringActivityId:chararray);
ext = FOREACH tweets GENERATE tweet#'createdAt' AS timestamp:long,
							  tweet#'user' AS user:map[],
							  tweet#'entities' AS entities:map[];
ext1 = FOREACH ext GENERATE timestamp,
							user#'id' AS mentioner:long,
						 	entities#'userMentions' AS mentions:tuple(map[]);
/* ext2 = FOREACH ext1 GENERATE timestamp, FLATTEN(mentions) AS mention:map[];
* ext3 = FOREACH ext2 {
*		c = mention#'id';
*		GENERATE timestamp, c;
*		};*/
/* ext3 = FOREACH ext2 GENERATE timestamp, mention#'id' AS mentioned:long;
* STORE ext3 INTO '$OUTPUTDIR'; */
STORE ext1 INTO '$OUTPUTDIR';