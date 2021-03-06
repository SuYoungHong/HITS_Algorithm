
-- ***************************************************
-- * Phase 1
-- ***************************************************


-- * set configurations and clear out old tables
USE web;
set hive.cli.print.header=true;

DROP TABLE temp_auth;
DROP TABLE temp_hub;
DROP TABLE prev_auth;
DROP TABLE prev_hub;
DROP TABLE Hive_Results_Auth;
DROP TABLE Hive_Results_Hub;



-- * initialize authority and hub table with scores of 1
CREATE TABLE temp_auth STORED AS ORC AS SELECT DISTINCT to_page AS page, CAST(1 AS DOUBLE) AS score FROM pagelinks;
CREATE TABLE temp_hub STORED AS ORC AS SELECT DISTINCT from_page AS page, CAST(1 AS DOUBLE) AS score FROM pagelinks;


-- * initialize last-step table
CREATE TABLE prev_auth STORED AS ORC AS SELECT * FROM temp_auth ORDER BY page asc;
CREATE TABLE prev_hub STORED AS ORC AS SELECT * FROM temp_hub ORDER BY page asc;


-- * initialize performance records to track convergence
CREATE TABLE Hive_Results_Auth (ts TIMESTAMP, change DOUBLE);
CREATE TABLE Hive_Results_Hub (ts TIMESTAMP, change DOUBLE);


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * initialize norm values
CREATE TABLE norm_auth AS SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
CREATE TABLE norm_hub AS SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;

-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 2 (intermediate)
-- ***************************************************


-- * update authority score
INSERT OVERWRITE TABLE temp_auth SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page;


-- * update hub score
INSERT OVERWRITE TABLE temp_hub SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page;


-- * update norm values
INSERT OVERWRITE TABLE norm_auth SELECT SQRT(SUM(score*score)) AS norm FROM temp_auth;
INSERT OVERWRITE TABLE norm_hub SELECT SQRT(SUM(score*score)) AS norm FROM temp_hub;


-- * normalize scores
INSERT OVERWRITE TABLE temp_auth SELECT temp_auth.page, temp_auth.score/norm_auth.norm AS score FROM norm_auth JOIN temp_auth;
INSERT OVERWRITE TABLE temp_hub SELECT temp_hub.page, temp_hub.score/norm_hub.norm AS score FROM norm_hub JOIN temp_hub;


-- * update performance records
INSERT INTO TABLE Hive_Results_Auth SELECT unix_timestamp() AS ts, SUM(ABS(prev_auth.score - temp_auth.score)) AS change FROM prev_auth JOIN temp_auth ON (prev_auth.page = temp_auth.page);
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_auth.score)) AS change FROM prev_hub JOIN temp_auth ON (prev_hub.page = temp_auth.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;


-- ***************************************************
-- * Phase 3 (finish)
-- ***************************************************


-- * save output to S3
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/AUTHScores/'  select * from temp_auth;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/HUBcores/'  select * from temp_hub;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/AUTHResults/'  select * from Hive_Results_Auth;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/HUBResults/'  select * from Hive_Results_Hub;


-- * clear old tables for new results
DROP TABLE final_auth;
DROP TABLE final_hub;

-- * rename temporary tables to final table
ALTER TABLE temp_auth RENAME TO final_auth;
ALTER TABLE temp_hub RENAME TO final_hub;


-- * drop tables
DROP TABLE norm_auth;
DROP TABLE norm_hub;




