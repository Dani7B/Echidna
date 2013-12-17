/*
* Simple code to read mentioned-by relationships from binary file and store them into HBase
*/

REGISTER '/home/daniele/Pig/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

mention = LOAD '$INPUTDIR/part*' USING BinStorage() AS (id:long, mentioned:long, ts:long);
mentionedBy = GROUP mention BY mentioned;
refined = FOREACH mentionedBy {
			reversed = FOREACH mention
				GENERATE CONCAT(CONCAT((chararray)mentioned,'_'),SUBSTRING(UnixToISO(ts),0,10)),
						 TOMAP((chararray)ts,id);
			GENERATE FLATTEN(reversed);
		};
		
STORE refined INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
		('t:*', '-caster HBaseBinaryConverter');