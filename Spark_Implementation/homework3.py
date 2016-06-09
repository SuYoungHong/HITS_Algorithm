# spark.yarn.executor.memoryOverhead, needs to be bigger, 
# but first try just increasing executor memory...\
	# okay, so this looks like FRACTION of head you use for 
	# like, random stuff? i don't know, just make it higher!


# start spark like this: 
# 		pyspark --executor-memory 7000M
#
#		pyspark --driver-memory 3000M --executor-memory 7000M --conf spark.yarn.executor.memoryOverhead=4024

from operator import add
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)



links = sc.textFile('s3://hits-data-pagelinks/links/links-simple-sorted.txt')
links = links.map(lambda x: x.split(": "))
links = links.map(lambda x: [(x[0], i) for i in x[1].split(' ')])
links = links.flatMap(lambda x: x)
links = links.map(lambda x: (x[0], x[1])).cache()

	# from , to

hubsRDD = links.map(lambda x: x[0]).distinct()
	# hubs is a characteristic of originating pages
hubsRDD = hubsRDD.map(lambda x: (x,1.0))
hubs_dict = hubsRDD.collectAsMap()
hubs = sc.broadcast(hubs_dict)

# update auths
updateAuths = links.map(lambda x: (x[1], hubs.value[x[0]])).reduceByKey(add)


auths_dict = updateAuths.collectAsMap()

del hubs_dict
hubs.unpersist()

auths_norm = 0.0
for i in auths_dict.keys():
	auths_norm += auths_dict[i]**2


auths_norm = auths_norm**0.5
for i in auths_dict.keys():
	auths_dict[i] = auths_dict[i]/auths_norm


auths = sc.broadcast(auths_dict)

# update hubs
updateHubs = links.map(lambda x: (x[0], auths.value[x[1]])).reduceByKey(add)

hubs_dict = updateHubs.collectAsMap()

#del auths_dict

	# !!!

# !!!! this is just to get the first iteration results! 
# when you get more, you NEED to uncomment the above!!!!

	# !!!

auths.unpersist()

hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm







############ for getting results################
#final_auths = sc.parallelize(auths_dict.items())
final_hubs = sc.parallelize(hubs_dict.items())

del auths_dict

lookup_table = sc.textFile('s3://hits-data-pagelinks/titles/titles-sorted.txt')
lookup_table = lookup_table.zipWithIndex()
lookup_table = lookup_table.map(lambda x: (str(x[1]),x[0]))
names_dict = lookup_table.collectAsMap()
names = sc.broadcast(names_dict)
#final_auths = final_auths.map(lambda x: (names.value[x[0]], x[1]))
final_hubs = final_hubs.map(lambda x: (names.value[x[0]], x[1])).cache()
del names_dict
names.unpersist()

#auth_results = sqlContext.createDataFrame(final_auths, ['page','auth'])
#sqlContext.registerDataFrameAsTable(auth_results, "table1")
#final1 = sqlContext.sql('SELECT * FROM table1 order by auth desc limit 20').collect()
#final1


hubs_results = sqlContext.createDataFrame(final_hubs, ['page','hubs'])
sqlContext.registerDataFrameAsTable(hubs_results, "table2")
final2 = sqlContext.sql('SELECT * FROM table2 order by hubs desc limit 20').collect()
final2

import operator
sorted_x = sorted(hubs_dict.items(), key=operator.itemgetter(1), reverse=True)
sorted_x[0:20]



############################### for more iterations

for i in range(0,6):
	hubs = sc.broadcast(hubs_dict)
	updateAuths = links.map(lambda x: (x[1], hubs.value[x[0]])).reduceByKey(add)
	auths_dict = updateAuths.collectAsMap()
	del hubs_dict
	hubs.unpersist()
	auths_norm = 0.0
	for i in auths_dict.keys():
		auths_norm += auths_dict[i]**2
	auths_norm = auths_norm**0.5
	for i in auths_dict.keys():
		auths_dict[i] = auths_dict[i]/auths_norm
	auths = sc.broadcast(auths_dict)
	updateHubs = links.map(lambda x: (x[0], auths.value[x[1]])).reduceByKey(add)
	hubs_dict = updateHubs.collectAsMap()
	del auths_dict
	auths.unpersist()
	hubs_norm = 0.0
	for i in hubs_dict.keys():
		hubs_norm += hubs_dict[i]**2
	hubs_norm = hubs_norm**0.5
	for i in hubs_dict.keys():
		hubs_dict[i] = hubs_dict[i]/hubs_norm


hubs = sc.broadcast(hubs_dict)
updateAuths = links.map(lambda x: (x[1], hubs.value[x[0]])).reduceByKey(add)
auths_dict = updateAuths.collectAsMap()
del hubs_dict
hubs.unpersist()
auths_norm = 0.0
for i in auths_dict.keys():
	auths_norm += auths_dict[i]**2


auths_norm = auths_norm**0.5
for i in auths_dict.keys():
	auths_dict[i] = auths_dict[i]/auths_norm


auths = sc.broadcast(auths_dict)
updateHubs = links.map(lambda x: (x[0], auths.value[x[1]])).reduceByKey(add)
hubs_dict = updateHubs.collectAsMap()
#del auths_dict
auths.unpersist()
hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm


##############################################################



import operator
sorted_x = sorted(hubs_dict.items(), key=operator.itemgetter(1), reverse=True)
sorted_x[0:20]

import operator
sorted_y = sorted(auths_dict.items(), key=operator.itemgetter(1), reverse=True)
sorted_y[0:20]






final_auths = sc.parallelize(auths_dict.items())
final_hubs = sc.parallelize(hubs_dict.items())

lookup_table = sc.textFile('s3://hits-data-pagelinks/titles/titles-sorted.txt')
lookup_table = lookup_table.zipWithIndex()
lookup_table = lookup_table.map(lambda x: (str(x[1]),x[0]))
names_dict = lookup_table.collectAsMap()
names = sc.broadcast(names_dict)
final_auths = final_auths.map(lambda x: (names.value[x[0]], x[1]))
final_hubs = final_hubs.map(lambda x: (names.value[x[0]], x[1]))
del names_dict
names.unpersist()

auth_results = sqlContext.createDataFrame(final_auths, ['page','auth'])
sqlContext.registerDataFrameAsTable(auth_results, "table1")
final1 = sqlContext.sql('SELECT * FROM table1 order by auth desc limit 20').collect()
final1


del auths_dict
auth_results.unpersist()
del auth_results

hubs_results = sqlContext.createDataFrame(final_hubs, ['page','hubs'])
sqlContext.registerDataFrameAsTable(hubs_results, "table2")
final2 = sqlContext.sql('SELECT * FROM table2 order by hubs desc limit 20').collect()
final2





from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from operator import add


links = sc.textFile('s3://hits-data-pagelinks/links/links-simple-sorted.txt')
links = links.map(lambda x: x.split(": "))
links = links.map(lambda x: [(x[0], i) for i in x[1].split(' ')])
links = links.flatMap(lambda x: x)
links = links.map(lambda x: (x[0], x[1]))

links = sqlContext.createDataFrame(links, ['from_page','to_page'])
sqlContext.registerDataFrameAsTable(links, "pagelinks")


hubsRDD = links.map(lambda x: x[0]).distinct()
hubsRDD = hubsRDD.map(lambda x: (x,1.0))
hubs = sqlContext.createDataFrame(hubsRDD, ['page','score'])
sqlContext.registerDataFrameAsTable(hubs, "hubs")

for i in range(0,8):
	auths = sqlContext.sql('SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page')
	hubs = sqlContext.sql('SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page')






links.count()
hubs = sc.broadcast(hubs_dict)
links.filter(lambda x: x[0] in hubs.value.keys()).count()
hubs.unpersist()
auths = sc.broadcast(auths_dict)
links.filter(lambda x: x[0] in auths.value.keys()).count()
auths.unpersist()





