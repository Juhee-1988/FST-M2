-- Load the input files from HDFS
inputFile1 = LOAD 'hdfs:///user/juhee/inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (character:chararray,lines:chararray)
inputFile2 = LOAD 'hdfs:///user/juhee/inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS (character:chararray,lines:chararray)
inputFile3 = LOAD 'hdfs:///user/juhee/inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS (character:chararray,lines:chararray)
-- Combine the input files
inputFiles = UNION inputFile1,inputFile2,inputFile3
-- Group lines using the character
GroupByCharacter = GROUP inputFiles BY character
-- Count the number of lines spoken by each character
CountByLines = FOREACH GroupByCharacter GENERATE $0, COUNT($1) AS lineCount
-- Order the line count in descending order
OrderByLineCount = ORDER CountByLines BY lineCount DESC
-- Delete the output folder
rmf hdfs:///user/juhee/projpigresults
-- Store the result in HDFS
STORE OrderByLineCount INTO 'inputFile1 = LOAD 'hdfs:///user/juhee/inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (character:chararray,lines:chararray) 

