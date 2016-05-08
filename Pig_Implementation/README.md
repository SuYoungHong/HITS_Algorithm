# HITS Pig Implementation

### run_scripts.sh
Runs algorithm. 
	* Concatenates different pig files to create intermediate pig script: HITS.pig with user specified number of iterations.
		* Set number of iterations in line 4:
		* seq 1 <your desired number of iterations> | xargs <etc.>
	* Runs HIVE_PrepForPig.sql to prepare Hive tables to store outputs
	* Then runs HITS.pig


### HIVE_PrepForPig.sql

Creates tables in Hive to accept output data from algorithm


### Pig_HITSpt1.pig

First part of final pig script. Initializes tables for beginning of algorithm. 



### Pig_HITSpt2.pig

Second part of final pig script. Is one iteration of the algorithm. 


### Pig_HITSpt3.pig

Third and final part of final pig script. Writes resultant Hubs, Authority, and Performance (comparing in scores in each iteration and time spent to compute) tables to S3 and Hive. 


### Pig_TemplateHITS.pig

Template for entire algorithm. 