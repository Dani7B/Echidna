/*
* Considering only the latest snapshot for each users,
* it computes a rank which show the number of followers in the first column and
* the number of users with that followersCount in the second column.
* This code stores the results into HBase.
*/

snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
snapshots = GROUP snap BY id;
ordered = FOREACH snapshots {
		orderedSnapshots = ORDER snap BY timestamp desc;
		latest = LIMIT orderedSnapshots 1;
		info = FOREACH latest GENERATE user#'lang' AS lang:chararray,
				 user#'friendsCount' AS friends:long,
				 user#'followersCount' AS followers:long;
		GENERATE FLATTEN(info);
		};
grouped = GROUP ordered BY LOWER(lang);
number = FOREACH grouped GENERATE group, SUM(ordered.friends), SUM(ordered.followers);
sorted = ORDER number BY $2 desc, $1 desc;
STORE sorted INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('languages:friends, languages:followers');