/*
* Simple code to count the amout of friends and followers for any language,
* taking into account all the snapshot of any user.
*/

SET default_parallel $REDUCERS;
snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
ff = FOREACH snap GENERATE user#'lang' AS (lang:chararray), user#'friendsCount' AS (friends:long), user#'followersCount' AS (followers:long);
grouped = GROUP ff BY LOWER(lang);
number = FOREACH grouped GENERATE group, SUM(ff.friends), SUM(ff.followers);
ordered = ORDER number BY $2 desc, $1 desc;
STORE ordered INTO '$OUTPUTDIR';