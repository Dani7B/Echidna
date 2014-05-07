/*
* Simple code to count the sum of friends and followers of the snapshots stored with even or odd id
*/

SET default_parallel $REDUCERS;
snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
ff = FOREACH snap GENERATE id, user#'friendsCount' AS (friends:long), user#'followersCount' AS (followers:long);
grouped = GROUP ff BY (id%2);
number = FOREACH grouped GENERATE group, SUM(ff.friends), SUM(ff.followers);
STORE number INTO '$OUTPUTDIR';