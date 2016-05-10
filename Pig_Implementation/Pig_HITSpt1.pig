-- ******************************************************
-- * Calculate HITS algorithm
-- ******************************************************

-- * Make sure to start pig in HCatalog mode
-- * 	pig -useHCatalog


-- ******************************************************
-- * load data from Hive
-- ******************************************************

PAGEIDS = LOAD 'web.pageids' USING org.apache.hive.hcatalog.pig.HCatLoader();

PAGELINKS = LOAD 'web.pagelinks' USING org.apache.hive.hcatalog.pig.HCatLoader();

SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET DEFAULT_PARALLEL 20;

-- ******************************************************
-- * initialize authority table and hubs table
-- ******************************************************

INIT_AUTH = FOREACH PAGEIDS GENERATE page_id, 1 AS auth;


INIT_HUB = FOREACH PAGEIDS GENERATE page_id, 1 AS hub; 


-- ******************************************************
-- * update authority using hub scores
-- ******************************************************

AUTH = JOIN INIT_HUB BY page_id, PAGELINKS BY from_page;

AUTH = GROUP AUTH BY to_page;

AUTH = FOREACH AUTH GENERATE group AS page_id, AUTH.INIT_HUB::hub AS hub;

AUTH = FOREACH AUTH GENERATE page_id, SUM(hub) AS new_auth, 1 AS old_auth;


-- ******************************************************
-- * update hubs using authoirty scores
-- ******************************************************

HUB = JOIN AUTH BY page_id, PAGELINKS BY to_page; 

HUB = GROUP HUB BY from_page; 

HUB = FOREACH HUB GENERATE group AS page_id, HUB.AUTH::new_auth AS auth;

HUB = FOREACH HUB GENERATE page_id, SUM(auth) AS new_hub, 1 AS old_hub;




-- ******************************************************
-- * normalize hub
-- ******************************************************

NORM_HUB = FOREACH HUB GENERATE new_hub * new_hub AS sq_hub;
NORM_HUB = GROUP NORM_HUB ALL;
NORM_HUB = FOREACH NORM_HUB GENERATE SUM(NORM_HUB.sq_hub) AS sq_sum;
NORM_HUB = FOREACH NORM_HUB GENERATE SQRT(sq_sum) AS norm;

HUB = JOIN HUB BY 1, NORM_HUB BY 1 USING 'replicated';
HUB = FOREACH HUB GENERATE HUB::page_id AS page_id, HUB::new_hub / NORM_HUB::norm AS new_hub, HUB::old_hub AS old_hub;


-- ******************************************************
-- * normalize authority
-- ******************************************************

NORM_AUTH = FOREACH AUTH GENERATE new_auth * new_auth AS sq_auth;
NORM_AUTH = GROUP NORM_AUTH ALL;
NORM_AUTH = FOREACH NORM_AUTH GENERATE SUM(NORM_AUTH.sq_auth) AS sq_sum;
NORM_AUTH = FOREACH NORM_AUTH GENERATE SQRT(sq_sum) AS norm;

AUTH = JOIN AUTH BY 1, NORM_AUTH BY 1 USING 'replicated';
AUTH = FOREACH AUTH GENERATE AUTH::page_id AS page_id, AUTH::new_auth / NORM_AUTH::norm AS new_auth, AUTH::old_auth AS old_auth;


-- ******************************************************
-- * calculate performance metrics
-- ******************************************************

PERF_AUTH = FOREACH AUTH GENERATE ABS(new_auth - old_auth) AS diff;
PERF_AUTH = GROUP PERF_AUTH ALL;
PERF_AUTH = FOREACH PERF_AUTH GENERATE SUM(PERF_AUTH) AS auth_dif;

PERF_HUB = FOREACH HUB GENERATE ABS(new_hub - old_hub) AS diff;
PERF_HUB = GROUP PERF_HUB ALL;
PERF_HUB = FOREACH PERF_HUB GENERATE SUM(PERF_HUB) AS hub_dif;

PERF = JOIN PERF_AUTH BY 1, PERF_HUB BY 1 USING 'replicated';
PERF = FOREACH PERF GENERATE CurrentTime() AS time, PERF_AUTH::auth_dif AS auth_dif, PERF_HUB::hub_dif AS hub_dif;

