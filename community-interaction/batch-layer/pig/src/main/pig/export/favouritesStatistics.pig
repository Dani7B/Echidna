/* 
* It yields the ranking of the most favourites users;
* if more than one user classifies for that position,
* prints the list. It stores the results into HBase
*/

snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
snapshots = GROUP snap BY id;
ordered = FOREACH snapshots {
		orderedSnapshots = ORDER snap BY timestamp desc;
		latest = LIMIT orderedSnapshots 1;
		info = FOREACH latest GENERATE id AS userId:long, user#'favouritesCount' AS favourites;
		GENERATE FLATTEN(info);
		};
grouped = GROUP ordered BY favourites;
result = FOREACH grouped GENERATE group AS favouritesCount:int, COUNT(ordered), ordered.userId;
sorted = ORDER result BY favouritesCount desc; -- favouritesCount MUST be int, if long Hadoop will expect an int anyway
STORE sorted INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('users:count, users:userId', '-caster HBaseBinaryConverter');