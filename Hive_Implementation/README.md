# HITS Implementation in Hive

Performs HITS algorithm using Hive tables, for a given number of iterations. 

Outputs Authority table, Hubs table, and performance table (change in scores between iterations) to Hive and S3. 

Algorithm is run through run_scripts.sh, which stiches sql code for algorithm into one file, HITS.sql from 3 sql files. 

### run_scripts.sh

* Stiches together HitsPt(s 1, 2, and 3) to create HITS.sql
* Then executes HITS.sql 
* Number of iteractions can be controlled through: 
* 	seq 1 20 | xarges -Inon cat HitsPt2.sql >> HITS.sql
* 		Change sequence number to control number of iterations

### HitsPt1.sql

* Sets up tables for algorithm and initializes first iteration

### HitsPt2.sql

* Performs intermediate iterations

### HitsPt3.sql

* Writes output to s3 and hive
* Removes intermediate tables 