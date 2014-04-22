/*
* Simple code to extract follow and mention relationships from Elasticsearch
*/

SET default_parallel $REDUCERS;

mention = LOAD '$INM/part-*' AS (mentioner:long, mentioned:long, timestamp:long);
STORE mention INTO '$MENTIONS' USING BinStorage();

follows = LOAD '$INF/part-*' AS (follower:long, followed:long, timestamp:long);
STORE follows INTO '$FOLLOWS' USING BinStorage();
