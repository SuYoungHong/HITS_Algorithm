
-- ***************************************************
-- * Phase 3 (finish)
-- ***************************************************


-- * save output to S3
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/AUTHScores/'  select * from temp_auth;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/HUBcores/'  select * from temp_hub;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/AUTHResults/'  select * from Hive_Results_Auth;
INSERT OVERWRITE DIRECTORY 's3://hits-data-pagelinks/HIVE_results/HUBResults/'  select * from Hive_Results_Hub;


-- * drop tables
DROP TABLE temp_auth;
DROP TABLE temp_hub;
DROP TABLE norm_auth;
DROP TABLE norm_hub;


-- * clear old tables for new results
DROP TABLE final_auth;
DROP TABLE final_hub;

-- * rename temporary tables to final table
ALTER TABLE temp_auth TO final_auth;
ALTER TABLE temp_hub TO final_hub;

