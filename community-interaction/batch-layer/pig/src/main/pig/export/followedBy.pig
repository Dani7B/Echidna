/*
* Simple code to read followed-by relationships from binary file and store them into HBase
*/

REGISTER '/home/daniele/Pig/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

follow = LOAD '$INPUTDIR/part*' USING BinStorage() AS (id:chararray, followed:chararray, ts:long);
followedBy = GROUP follow BY followed;
refined = FOREACH followedBy {
			reversed = FOREACH follow 
				GENERATE CONCAT(CONCAT(followed,'_'),SUBSTRING(UnixToISO(ts),0,10)), TOMAP((chararray)ts,id);
			GENERATE FLATTEN(reversed);
		};

STORE refined INTO 'hbase://$OUTPUTDIR' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage
			('t:*', '-caster HBaseBinaryConverter');