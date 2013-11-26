/*
* Simple code to count the amout of friends and followers for any language,
* taking into account all the snapshot of any user, and store them into HBase.
*/

snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
ff = FOREACH snap GENERATE user#'lang' AS (lang:chararray), user#'friendsCount' AS (friends:long), user#'followersCount' AS (followers:long);
grouped = GROUP ff BY LOWER(lang);
number = FOREACH grouped GENERATE group, SUM(ff.friends), SUM(ff.followers);
sorted = ORDER number BY $2 desc, $1 desc;
STORE sorted INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('languages:friends, languages:followers');