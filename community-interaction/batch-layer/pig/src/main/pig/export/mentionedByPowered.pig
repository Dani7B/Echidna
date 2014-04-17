/*
* Simple code to load tweets from HDFS, extract mentioner-mentioned information out of it
* and store it into HBase
*/

SET default_parallel $REDUCERS;
REGISTER '/home/daniele/Pig/pig-0.12.1/contrib/piggybank/java/piggybank.jar';
REGISTER '/home/daniele/piggyback-1.0-SNAPSHOT.jar';
DEFINE FromTupleToBag org.piggyback.FromTupleToBag();
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE HBaseStorage org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-caster HBaseBinaryConverter');

tweets = LOAD '$INPUTDIR/part*' USING BinStorage() AS (id:map[],tweet:map[],monitoringActivityId:chararray);
ext = FOREACH tweets GENERATE tweet#'createdAt' AS timestamp:long,
							  tweet#'user' AS user:map[],
							  tweet#'entities' AS entities:map[];
							  
ext1 = FOREACH ext GENERATE timestamp,
							user#'id' AS mentioner:long,
						 	entities#'userMentions' AS mentions:tuple(map[]);
						 	
ext2 = FOREACH ext1 GENERATE timestamp, mentioner, FromTupleToBag(mentions) AS mentionsBag:bag{t:(p:map[])};
ext3 = FOREACH ext2 GENERATE mentioner, FLATTEN(mentionsBag) AS mentioned:map[], timestamp;
singleLine = FOREACH ext3 GENERATE mentioner, mentioned#'id' AS mentionedId:long, timestamp;

/* Code in common to all the jobs */

mentionedBy = GROUP singleLine BY mentionedId;

simple = FOREACH mentionedBy {
			reversed = FOREACH singleLine
				GENERATE mentionedId, timestamp, mentioner;
			GENERATE FLATTEN(reversed);
		};

mentioned_group = GROUP simple BY mentionedId;



/* Work to compute mentionedByMonth view */

month = FOREACH mentioned_group {
		  mentioned_month = FOREACH simple
					GENERATE (CONCAT(CONCAT((chararray)mentionedId,'_'), SUBSTRING(UnixToISO(timestamp),0,7)), mentioner) AS couple;
			GENERATE FLATTEN(mentioned_month);
		};
		
monthLine = GROUP month BY couple;
montly = FOREACH monthLine
				GENERATE group.$0, TOMAP((chararray) group.$1, (int)COUNT(month));
		
STORE montly INTO 'hbase://$MONTHLY' USING HBaseStorage;



/* Work to compute mentionedByDay view */

day = FOREACH mentioned_group {
		  mentioned_day = FOREACH simple
				GENERATE (CONCAT(CONCAT((chararray)mentionedId,'_'), SUBSTRING(UnixToISO(timestamp),0,10)), mentioner) AS couple, timestamp;
			GENERATE FLATTEN(mentioned_day);
		};
		
dayLine = GROUP day BY couple;
daily = FOREACH dayLine
				GENERATE group.$0, TOMAP((chararray) group.$1, (int)COUNT(day));

STORE daily INTO 'hbase://$DAILY' USING HBaseStorage;		



/* Work to compute mentionedBy */

mentionedByGroup = GROUP day BY couple;
global = FOREACH mentionedByGroup {
				part = FOREACH day
					GENERATE couple.$0, TOMAP((chararray)timestamp,couple.$1);
				GENERATE FLATTEN(part);
			};

STORE global INTO 'hbase://$GLOBAL' USING HBaseStorage;