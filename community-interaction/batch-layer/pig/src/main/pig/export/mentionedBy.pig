/*
* Simple code to read mentioned-by relationships from binary file and store them into HBase
*/

mention = LOAD '$INPUTDIR/part*' USING BinStorage() AS (id:chararray, mentioned:chararray, ts:long);
mentionedBy = GROUP mention BY mentioned;
refined = FOREACH mentionedBy {
			reversed = FOREACH mention GENERATE TOMAP(id,ts);
			GENERATE group, FLATTEN(reversed);
		};

STORE refined INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('users:*', '-caster HBaseBinaryConverter');