-- ******************************************************
-- * Save outputs:
-- * 	Hub table
-- * 	Auth table
-- * 	Performance table
-- ******************************************************

STORE PERF INTO 's3://hits-data-pagelinks/PIG_results/perf_table/' USING PigStorage(',');
STORE AUTH INTO 's3://hits-data-pagelinks/PIG_results/auth_table/' USING PigStorage(',');
STORE HUB INTO 's3://hits-data-pagelinks/PIG_results/hub_table/' USING PigStorage(',');

STORE PERF INTO 'web.results_perf' USING org.apache.hive.hcatalog.pig.HCatStorer();
STORE AUTH INTO 'web.results_auth' USING org.apache.hive.hcatalog.pig.HCatStorer();
STORE HUB INTO 'web.results_hub' USING org.apache.hive.hcatalog.pig.HCatStorer();


