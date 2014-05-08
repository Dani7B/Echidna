/*
* Simple code to read mentioned-by relationships from binary file and store them into HBase
*/
SET default_parallel $REDUCERS;
REGISTER '$PIGGYBANK/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE HBaseStorage org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-caster HBaseBinaryConverter');

/* Code in common to all the jobs */

mention = LOAD '$INPUTDIR/part*' USING BinStorage() AS (mentioner:long, mentioned:long, ts:long);

simple = FOREACH mention GENERATE mentioned, mentioner, ts;
mentioned_group = GROUP simple BY mentioned;

/* Work to compute mentionedByMonth view */

month = FOREACH mentioned_group {
		  mentioned_month = FOREACH simple
					GENERATE (CONCAT(CONCAT((chararray)mentioned,'_'), SUBSTRING(UnixToISO(ts),0,7)), mentioner) AS couple;
			GENERATE FLATTEN(mentioned_month);
		};
		
monthLine = GROUP month BY couple;
montly = FOREACH monthLine
				GENERATE group.$0, TOMAP((chararray) group.$1, (int)COUNT(month));
		
STORE montly INTO '$MONTHLY' USING HBaseStorage;



/* Work to compute mentionedByDay view */

day = FOREACH mentioned_group {
		  mentioned_day = FOREACH simple
				GENERATE (CONCAT(CONCAT((chararray)mentioned,'_'), SUBSTRING(UnixToISO(ts),0,10)), mentioner) AS couple, ts;
			GENERATE FLATTEN(mentioned_day);
		};
		
dayLine = GROUP day BY couple;
daily = FOREACH dayLine
				GENERATE group.$0, TOMAP((chararray) group.$1, (int)COUNT(day));

STORE daily INTO '$DAILY' USING HBaseStorage;		



/* Work to compute mentionedBy */

mentionedByGroup = GROUP day BY couple;
global = FOREACH mentionedByGroup {
				part = FOREACH day
					GENERATE couple.$0, TOMAP((chararray)ts,couple.$1);
				GENERATE FLATTEN(part);
			};

STORE global INTO '$GLOBAL' USING HBaseStorage;