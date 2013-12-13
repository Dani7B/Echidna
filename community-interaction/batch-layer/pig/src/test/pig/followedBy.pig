/*
* Simple code to read followed-by relationships from binary file and store them into HBase
*/

follow = LOAD '$INPUTDIR/part*' AS (id:chararray, followed:chararray, ts:long);
followedBy = GROUP follow BY followed;
refined = FOREACH followedBy {
			reversed = FOREACH follow 
				GENERATE followed, TOMAP(id,ts);
			GENERATE FLATTEN(reversed);
		};

STORE refined INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('t:*', '-caster HBaseBinaryConverter');