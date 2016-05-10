#!/bin/bash

rm HITS.sql

cat HitsPt1.sql >> HITS.sql
seq 1 20 | xargs -Inone cat HitsPt2.sql >> HITS.sql
cat HitsPt3.sql >> HITS.sql

hive -f HITS.sql >> err_log.txt

echo 'done'

