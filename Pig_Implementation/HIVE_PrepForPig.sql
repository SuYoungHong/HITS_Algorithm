-- *****************************************
-- * Prepare Hive tables for results of Pig
-- * HITS algo
-- *****************************************

USE web;

DROP TABLE results_perf;
DROP TABLE results_hub;
DROP TABLE results_auth;



CREATE TABLE results_perf (time TIMESTAMP, auth_dif DOUBLE, hub_dif DOUBLE);

CREATE TABLE results_hub (page_id STRING, new_hub DOUBLE, old_hub DOUBLE);

CREATE TABLE results_auth (page_id STRING, new_auth DOUBLE, old_auth DOUBLE);

