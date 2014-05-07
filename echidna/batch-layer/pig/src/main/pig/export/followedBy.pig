/*
* Simple code to read "X follows Y" relationships from binary file and store them appropriately into HBase tables
*/
SET default_parallel $REDUCERS;
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


/* Work to compute "whose followers follow" view */

DEFINE DUPLICATE(in) RETURNS out {
        $out = FOREACH $in GENERATE *;
};
	
duplicated = DUPLICATE(follow);

joined = JOIN follow BY follower, duplicated BY follower;

jGrouped = GROUP joined BY follow::followed;
couples = FOREACH jGrouped {
			coupled = FOREACH joined
					GENERATE (follow::followed,duplicated::followed) AS couple;
				GENERATE FLATTEN(coupled);
		};

tuples = GROUP couples by couple;
wff = FOREACH tuples GENERATE group.$0, TOMAP((chararray)group.$1,(int)COUNT(couples));

whoseFollowersFollow = STORE wff INTO 'hbase://$WHOSEFOLLOWERSFOLLOW' USING HBaseStorage;



/* Work to compute "whose followers are followed by" view */

joinedF = JOIN follow BY followed, duplicated BY follower;

jGroupedF = GROUP joinedF BY follow::follower;
couplesF = FOREACH jGroupedF {
			coupledF = FOREACH joinedF
					GENERATE (follow::follower,duplicated::followed) AS coupleF;
				GENERATE FLATTEN(coupledF);
		};

tuplesF = GROUP couplesF by coupleF;
wfafb = FOREACH tuplesF GENERATE group.$0, TOMAP((chararray)group.$1,(int)COUNT(couplesF));

whoseFollowersAreFollowedBy = STORE wfafb INTO 'hbase://$WHOSEFOLLOWERSAREFOLLOWEDBY' USING HBaseStorage;	