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