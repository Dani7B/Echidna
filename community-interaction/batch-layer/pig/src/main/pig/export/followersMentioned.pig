/*
* Simple code to read "X follows Y" and "X mentioned Y" relationships from binary file and store them appropriately into HBase tables
*/

REGISTER '/home/daniele/Pig/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE HBaseStorage org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-caster HBaseBinaryConverter');

follow = LOAD '$FOLLOWS/part*' USING BinStorage() AS (follower:long, followed:long, ts:long);
mention = LOAD '$MENTIONS/part*' USING BinStorage() AS (mentioner:long, mentioned:long, ts:long);

joined = JOIN follow BY follower, mention BY mentioner;

jGrouped = GROUP joined BY mention::mentioned;

/* Work to compute wfmmonthly view */

monthly = FOREACH jGrouped {
			coupled = FOREACH joined
				GENERATE (CONCAT(CONCAT((chararray)mention::mentioned,'_'), SUBSTRING(UnixToISO(mention::ts),0,7)),
					follow::followed) AS tup;
				GENERATE FLATTEN(coupled);
		};

mGrouped = GROUP monthly BY tup;
mCounted = FOREACH mGrouped GENERATE group.$0, TOMAP((chararray)group.$1,(int)COUNT(monthly));

whoseFollowersMentionedMonthly = STORE mCounted INTO 'hbase://$WHOSEFOLLOWERSMENTIONEDMONTHLY' USING HBaseStorage;	


/* Work to compute wfmdaily view */

daily = FOREACH jGrouped {
			Dcoupled = FOREACH joined
				GENERATE (CONCAT(CONCAT((chararray)mention::mentioned,'_'), SUBSTRING(UnixToISO(mention::ts),0,10)),
					follow::followed) AS tupD;
				GENERATE FLATTEN(Dcoupled);
		};

dGrouped = GROUP daily BY tupD;
dCounted = FOREACH dGrouped GENERATE group.$0, TOMAP((chararray)group.$1,(int)COUNT(daily));

whoseFollowersMentionedDaily = STORE mCounted INTO 'hbase://$WHOSEFOLLOWERSMENTIONEDDAILY' USING HBaseStorage;	

