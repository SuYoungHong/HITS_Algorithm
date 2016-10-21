#!/bin/bash

cat Pig_HITSpt1.pig >> HITS.pig
seq 1 50 | xargs -Inone cat Pig_HITSpt2.pig >> HITS.pig
cat Pig_HITSpt3.pig >> HITS.pig

pig -useHCatalog HITS.pig >> err_log.txt

echo 'done'

