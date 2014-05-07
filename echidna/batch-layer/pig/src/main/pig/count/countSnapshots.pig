/* 
* Simple code to count the number of snapshots stored for each user
*/

SET default_parallel $REDUCERS;
snapshot = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
snapshots = GROUP snapshot BY id;
counter = FOREACH snapshots GENERATE group, COUNT(snapshot);
STORE counter INTO '$OUTPUTDIR';