# spark.yarn.executor.memoryOverhead, needs to be bigger, 
# but first try just increasing executor memory...\
	# okay, so this looks like FRACTION of head you use for 
	# like, random stuff? i don't know, just make it higher!


# start spark like this: 
# 		pyspark --executor-memory 7000M
#
#		pyspark --executor-memory 7000M --conf spark.yarn.executor.memoryOverhead=3024

from operator import add

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

del auths_dict
auths.unpersist()

hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm


#############
############# iteration 2
#############


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

del auths_dict
auths.unpersist()

hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm





#############
############# iteration 3
#############


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

del auths_dict
auths.unpersist()

hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm






#############
############# iteration 4
#############


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
auths.unpersist()

hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm



import operator
sorted_hubs = sorted(hubs_dict.items(), key=operator.itemgetter(1))

sorted_auths = sorted(auths_dict.items(), key=operator.itemgetter(1))




###############################

for i in range(0,4):
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



final_auths = sc.parallelize(auths_dict)
final_hubs = sc.parallelize(hubs_dict)

lookup_table = sc.textFile('s3://hits-data-pagelinks/titles/titles-sorted.txt')
lookup_table = lookup_table.zipWithIndex


