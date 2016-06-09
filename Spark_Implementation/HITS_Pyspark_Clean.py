# spark.yarn.executor.memoryOverhead, needs to be bigger, 
# but first try just increasing executor memory...\
	# okay, so this looks like FRACTION of head you use for 
	# like, random stuff? i don't know, just make it higher!


# start spark like this: 
# 		pyspark --executor-memory 7000M
#
#		pyspark --driver-memory 3000M --executor-memory 7000M --conf spark.yarn.executor.memoryOverhead=4024

# decide how many iterations you want
iterations = 6

# set up your links RDD
links = sc.textFile('s3://hits-data-pagelinks/links/links-simple-sorted.txt')
links = links.map(lambda x: x.split(": "))
links = links.map(lambda x: [(x[0], i) for i in x[1].split(' ')])
links = links.flatMap(lambda x: x)
links = links.map(lambda x: (x[0], x[1])).cache()

# initialize a hubs dict
hubsRDD = links.map(lambda x: x[0]).distinct()
hubsRDD = hubsRDD.map(lambda x: (x,1.0))
hubs_dict = hubsRDD.collectAsMap()
hubs = sc.broadcast(hubs_dict)

# update auths
updateAuths = links.map(lambda x: (x[1], hubs.value[x[0]])).reduceByKey(add)
auths_dict = updateAuths.collectAsMap()

# free up space
del hubs_dict
hubs.unpersist()

# normalize auth scores
auths_norm = 0.0
for i in auths_dict.keys():
	auths_norm += auths_dict[i]**2


auths_norm = auths_norm**0.5
for i in auths_dict.keys():
	auths_dict[i] = auths_dict[i]/auths_norm

# update hubs
auths = sc.broadcast(auths_dict)
updateHubs = links.map(lambda x: (x[0], auths.value[x[1]])).reduceByKey(add)
hubs_dict = updateHubs.collectAsMap()

# free up space
del auths_dict
auths.unpersist()

# normalize hub scores
hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm


########## iterate however many times you want ##########
for i in range(0,iterations-2):
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


# update auth for the last time
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

# update hubs for last time
auths = sc.broadcast(auths_dict)
updateHubs = links.map(lambda x: (x[0], auths.value[x[1]])).reduceByKey(add)
hubs_dict = updateHubs.collectAsMap()
#del auths_dict - we need to do this outside the loop so we have our auth_dict
auths.unpersist()
hubs_norm = 0.0
for i in hubs_dict.keys():
	hubs_norm += hubs_dict[i]**2


hubs_norm = hubs_norm**0.5
for i in hubs_dict.keys():
	hubs_dict[i] = hubs_dict[i]/hubs_norm


######### write to disk ##########

# load final hub and auth scores as RDD
authsRDD = sc.parallelize(auths_dict.items())
hubsRDD = sc.parallelize(hubs_dict.items())

authsRDD = authsRDD.coalesce(1)
authsRDD.saveAsTextFile("s3://your location auths")

hubsRDD = hubsRDD.coalesce(1)
hubsRDD.saveAsTextFile("s3://your location hubs")

