/*
* Simple code to read "X follows Y" relationships from binary file and store them appropriately into HBase tables
*/

DEFINE HBaseStorage org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-caster HBaseBinaryConverter');

follow = LOAD '$INPUTDIR/part*' USING BinStorage() AS (follower:long, followed:long, ts:long);


/* Work to compute followedBy view */

followedBy = GROUP follow BY followed;
upsideDown = FOREACH followedBy {
			reversed = FOREACH follow 
				GENERATE followed, TOMAP((chararray)follower,ts);
			GENERATE FLATTEN(reversed);
		};
		
STORE upsideDown INTO 'hbase://$FOLLOWEDBY' USING HBaseStorage;



/* Work to compute follow view */

follows = GROUP follow BY follower;
downsideUp = FOREACH follows {
			straight = FOREACH follow 
				GENERATE follower, TOMAP((chararray)followed,ts);
			GENERATE FLATTEN(straight);
			};

STORE downsideUp INTO 'hbase://$FOLLOW' USING HBaseStorage;


/* Work to compute "whose follower follow" view */

DEFINE DUPLICATE(in) RETURNS out {
        $out = FOREACH $in GENERATE *;
};
	
duplic = DUPLICATE(follow);

joined = JOIN follow BY follower, duplic BY follower;

jGrouped = GROUP joined BY follow::followed;
couples = FOREACH jGrouped {
			coupled = FOREACH joined
					GENERATE (follow::followed,duplic::followed) AS couple;
				GENERATE FLATTEN(coupled);
		};

tuples = GROUP couples by couple;
counted = FOREACH tuples GENERATE group AS tup, COUNT(couples) AS counter;

cGrouped = GROUP counted BY tup;
wff = FOREACH cGrouped {
			row = FOREACH counted 
				GENERATE tup.$0, TOMAP((chararray)tup.$1,counter);
			GENERATE FLATTEN(row);
			};

whoseFollowersFollow = STORE wff INTO 'hbase://$WHOSEFOLLOWERSFOLLOW' USING HBaseStorage;	