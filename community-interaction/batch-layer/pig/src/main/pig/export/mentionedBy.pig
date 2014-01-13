/*
* Simple code to read mentioned-by relationships from binary file and store them into HBase
*/

REGISTER '/home/daniele/Pig/pig-0.12.0/contrib/piggybank/java/piggybank.jar';
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE HBaseStorage org.apache.pig.backend.hadoop.hbase.HBaseStorage('t:*', '-caster HBaseBinaryConverter');

/* Code in common to all the jobs */

mention = LOAD '$INPUTDIR/part*' USING BinStorage() AS (mentioner:long, mentioned:long, ts:long);
mentionedBy = GROUP mention BY mentioned;

simple = FOREACH mentionedBy {
			reversed = FOREACH mention
				GENERATE mentioned, ts, mentioner;
			GENERATE FLATTEN(reversed);
		};

mentioned_group = GROUP simple BY mentioned;



/* Work to compute mentionedByMonth view */

month = FOREACH mentioned_group {
		  mentioned_month = FOREACH simple
					GENERATE (CONCAT(CONCAT((chararray)mentioned,'_'), SUBSTRING(UnixToISO(ts),0,7)) , mentioner) AS couple;
			GENERATE FLATTEN(mentioned_month);
		};
		
monthLine = GROUP month BY couple;
newMonthLine = FOREACH monthLine
			GENERATE group.$0 AS key:chararray, group.$1 AS mentioner:long, COUNT(month) AS counter:long;

groupedMonthLine = GROUP newMonthLine BY key;
montly = FOREACH groupedMonthLine {
			part = FOREACH newMonthLine
					GENERATE key, TOMAP((chararray)mentioner,counter);
				GENERATE FLATTEN(part);
		};
		
STORE montly INTO 'hbase://$MONTHLY' USING HBaseStorage;



/* Work to compute mentionedByDay view */

day = FOREACH mentioned_group {
		  mentioned_day = FOREACH simple
					GENERATE (CONCAT(CONCAT((chararray)mentioned,'_'), SUBSTRING(UnixToISO(ts),0,10)) , mentioner) AS couple,
					CONCAT(CONCAT((chararray)mentioned,'_'), SUBSTRING(UnixToISO(ts),0,10)) AS key:chararray, mentioner, ts;
			GENERATE FLATTEN(mentioned_day);
		};
		
dayLine = GROUP day BY couple;
newDayLine = FOREACH dayLine
			GENERATE group.$0 AS key:chararray, group.$1 AS mentioner:long, COUNT(day) AS counter:long;

groupedDayLine = GROUP newDayLine BY key;
daily = FOREACH groupedDayLine {
			part = FOREACH newDayLine
					GENERATE key, TOMAP((chararray)mentioner,counter);
				GENERATE FLATTEN(part);
		};

STORE daily INTO 'hbase://$DAILY' USING HBaseStorage;		



/* Work to compute mentionedBy */

mentionedByGroup = GROUP day BY key;
global = FOREACH mentionedByGroup {
				part = FOREACH day
					GENERATE key, TOMAP((chararray)ts,mentioner);
				GENERATE FLATTEN(part);
			};

STORE global INTO 'hbase://$GLOBAL' USING HBaseStorage;