/* 
* Simple code to count the number of snapshots stored with even or odd id
* and store them into HBase
*/

snap = LOAD '$INPUTDIR/part*' USING BinStorage() AS (user:map[],timestamp:long,id:long);
grouped = GROUP snap BY (id%2);
number = FOREACH grouped GENERATE group, COUNT(snap);
STORE number INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('snapshots:counter');