-- Considering only the latest snapshot for each users,
-- it computes statistics about the defaultProfile and defaultProfileImage boolean values
-- and stores them into HBase

snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
snapshots = GROUP snap BY id;
ordered = FOREACH snapshots {
		orderedSnapshots = ORDER snap BY timestamp desc;
		latest = LIMIT orderedSnapshots 1;
		info = FOREACH latest GENERATE (user#'defaultProfile',user#'defaultProfileImage') AS couple;
		GENERATE FLATTEN(info);
		};
dfp = GROUP ordered BY couple;
number = FOREACH dfp GENERATE group, COUNT(ordered);
STORE number INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('users:defaults');