/*
* Simple code to read mentioned-by relationships from binary file and store them into HBase
*/

REGISTER '/home/daniele/Pig/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE ISOToDay org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay();


mention = LOAD '$INPUTDIR/part*' USING BinStorage() AS (id:chararray, mentioned:chararray, ts:long);
mentionedBy = GROUP mention BY mentioned;
refined = FOREACH mentionedBy {
			reversed = FOREACH mention
				GENERATE CONCAT(CONCAT(mentioned,'_'),SUBSTRING(UnixToISO(ts),0,10)), TOMAP((chararray)ts,id);
			GENERATE FLATTEN(reversed);
		};
		
STORE refined INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
		('t:*', '-caster HBaseBinaryConverter');