-- ******************************************************
-- * Read raw CSVs from S3 and create ORC tables in Hive
-- ******************************************************

-- * Set params

set hive.exec.orc.default.block.size=268435456;
set hive.cli.print.header=true;

-- * Set up database structure
CREATE DATABASE web;

USE web;


-- ******************************************************
-- * Create pageIDs table
-- ******************************************************

DROP TABLE temp_pageIDs;
DROP TABLE pageIDs;

CREATE EXTERNAL TABLE temp_pageIDs 
		(page_name STRING) 
		LOCATION 's3://hits-data-pagelinks/titles/';

-- * condensed form for copypasting
-- 		CREATE EXTERNAL TABLE temp_pageIDs (page_name STRING) LOCATION 's3://hits-data-pagelinks/titles/';

CREATE TABLE pageIDs (page_ID STRING, page_name STRING) STORED AS ORC;

INSERT INTO TABLE pageIDs 
		SELECT ROW_NUMBER() 
		OVER(order by page_name) AS page_ID, page_name 
		FROM temp_pageIDs;

-- * condensed form for copypasting
-- 		INSERT INTO TABLE pageIDs SELECT ROW_NUMBER() OVER(order by page_name) AS page_ID, page_name FROM temp_pageIDs;

DROP TABLE temp_pageIDs;


-- ******************************************************
-- * Create pagelinks table
-- ******************************************************

DROP TABLE temp_pagelinks;
DROP TABLE pagelinks;


CREATE EXTERNAL TABLE temp_pagelinks 
	(from_page STRING, to_array ARRAY<STRING>)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ": " 
	COLLECTION ITEMS TERMINATED BY " " 
	LOCATION 's3://hits-data-pagelinks/links/';

-- * condensed form for copypasting
-- 		CREATE EXTERNAL TABLE temp_pagelinks (from_page STRING, to_array ARRAY<STRING>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ": " COLLECTION ITEMS TERMINATED BY " " LOCATION 's3://hits-data-pagelinks/links/';


CREATE TABLE pagelinks (from_page STRING, to_page STRING) STORED AS ORC;

INSERT INTO TABLE pagelinks 
		SELECT from_page, to_page 
		FROM temp_pagelinks 
		LATERAL VIEW explode(to_array) arr_table AS to_page 
		WHERE to_page != '';

-- * condensed form for copypasting
-- 		INSERT INTO TABLE pagelinks SELECT from_page, to_page from temp_pagelinks LATERAL VIEW explode(to_array) arr_table AS to_page WHERE to_page != '';

DROP TABLE temp_pagelinks;




