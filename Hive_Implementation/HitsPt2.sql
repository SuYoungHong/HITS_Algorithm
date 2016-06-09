
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
INSERT INTO TABLE Hive_Results_Hub SELECT unix_timestamp() AS ts, SUM(ABS(prev_hub.score - temp_hub.score)) AS change FROM prev_hub JOIN temp_hub ON (prev_hub.page = temp_hub.page);


-- * update last-step tables with current values
INSERT OVERWRITE TABLE prev_auth SELECT * FROM temp_auth;
INSERT OVERWRITE TABLE prev_hub SELECT * FROM temp_hub;

