/*
* Simple code to extract follow and mention relationships from Elasticsearch
*/

SET default_parallel $REDUCERS;

mention = LOAD '$INF/part-*' AS (mentioner:long, mentioned:long, timestamp:long);
STORE mention INTO '$MENTIONS' USING BinStorage();

follows = LOAD '$INM/part-*' AS (follower:long, followed:long, timestamp:long);
STORE follows INTO '$FOLLOWS' USING BinStorage();
