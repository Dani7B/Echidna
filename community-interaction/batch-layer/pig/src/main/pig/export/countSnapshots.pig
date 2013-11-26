-- Simple code to count the number of snapshots stored for each user
-- and store them into HBase

snapshot = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
snapshots = GROUP snapshot BY id;
result = FOREACH snapshots GENERATE group, COUNT(snapshot) AS counter;
STORE result INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('snapshots:counter');